import math
import json
import os
from collections import defaultdict
from functools import lru_cache
from typing import Dict, List, Tuple, Optional
import time
import pycountry
import pycountry_convert as pc
from bson import ObjectId
from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
from rich.progress import track

# MongoDB connection settings
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"
BATCH_SIZE = 500
CHECKPOINT_FILE = "matcher_checkpoint.json"

# Global console instance
console = Console()

client = MongoClient(MONGO_URI)
db = client[DATABASE]
shipments = db["trademo_entities"]
entities = db["opencorporates_entities"]
index = db["entity_token_index_final"]

# Import jurisdiction neighborhoods
from jurisdiction_neighborhood import JURISDICTION_NEIGHBORHOODS

# Common business words that should have lower weight
COMMON_BUSINESS_TERMS = {
    "LIMITED", "LTD", "PRIVATE", "PVT", "CORPORATION", "CORP",
    "COMPANY", "CO", "ENTERPRISE", "ENTERPRISES", "TRADING",
    "HOLDINGS", "GROUP", "INDUSTRIAL", "INDUSTRIES", "INC",
    "INCORPORATED", "LLC", "LLP", "GMBH", "AG", "SA", "SL",
    "BV", "NV", "OY", "AB", "SPA", "PTY",
}

class JurisdictionMatcher:
    """Handles jurisdiction matching and normalization."""

    def __init__(self):
        self.special_mappings = {
            "HONG KONG": "HK",
            "UNITED STATES": "US",
            "UNITED KINGDOM": "GB",
            "BRITAIN": "GB",
            "USA": "US",
            "UK": "GB",
            "TAIWAN": "TW",
            "VIETNAM": "VN",
        }

    @lru_cache(maxsize=1000)
    def normalize_jurisdiction(self, value: str) -> str:
        """Convert any jurisdiction string to a two-letter country code."""
        if not isinstance(value, str):
            return "XX"

        cleaned_value = value.strip().upper().replace(".", "")

        # Handle empty strings
        if not cleaned_value:
            return "XX"

        # Special mappings
        if cleaned_value in self.special_mappings:
            return self.special_mappings[cleaned_value]

        # Already a two-letter code
        if len(cleaned_value) == 2:
            try:
                country = pycountry.countries.get(alpha_2=cleaned_value)
                if country:
                    return country.alpha_2
            except LookupError:
                pass

        # Try pycountry exact match
        try:
            country = pycountry.countries.get(name=cleaned_value.title())
            if country:
                return country.alpha_2
        except LookupError:
            pass

        # Try pycountry_convert
        try:
            alpha2 = pc.country_name_to_country_alpha2(cleaned_value.title())
            return alpha2
        except KeyError:
            pass

        # Partial matches as last resort
        for country in pycountry.countries:
            if hasattr(country, "name") and cleaned_value in country.name.upper():
                return country.alpha_2
            if hasattr(country, "official_name") and cleaned_value in country.official_name.upper():
                return country.alpha_2
            if hasattr(country, "common_name") and cleaned_value in country.common_name.upper():
                return country.alpha_2

        return "XX"

    def calculate_jurisdiction_score(
        self, shipment_country: str, entity_jurisdiction: str, base_score: float
    ) -> float:
        """Calculate jurisdiction matching score with regional consideration."""
        if not shipment_country or not entity_jurisdiction:
            return 0
        
        entity_jurisdiction = self.normalize_jurisdiction(entity_jurisdiction)
        normalized_shipment = self.normalize_jurisdiction(shipment_country)
        
        if normalized_shipment == "XX":
            return 0

        # Exact jurisdiction match is highly significant
        if normalized_shipment == entity_jurisdiction:
            return 8.0  # Strong boost for exact jurisdiction match

        # Check neighborhood relationship
        if normalized_shipment in JURISDICTION_NEIGHBORHOODS:
            nearby = JURISDICTION_NEIGHBORHOODS[normalized_shipment].nearby_jurisdictions
            if entity_jurisdiction in nearby:
                return 4.0  # Moderate boost for neighboring jurisdictions

        return -1.0  # Small penalty for unrelated jurisdictions

class WordMatcher:
    """Handles sophisticated word matching and scoring."""

    @staticmethod
    def tokenize(text: str) -> List[str]:
        """Convert text to normalized tokens."""
        return [word.strip().upper() for word in text.split() if word.strip()]

    @staticmethod
    def calculate_word_importance(word: str) -> float:
        """Calculate importance weight for a word."""
        if word in COMMON_BUSINESS_TERMS:
            return 0.3
        return 1.0

    def calculate_word_match_score(
        self, shipment_tokens: List[str], entity_tokens: List[str]
    ) -> Tuple[float, Dict[str, bool]]:
        """
        Calculate detailed word matching score.
        Returns score and match details.
        """
        score = 0.0
        match_details = {}

        # Convert to sets for intersection operations
        shipment_set = set(shipment_tokens)
        entity_set = set(entity_tokens)

        # Score exact matches
        for word in shipment_set:
            importance = self.calculate_word_importance(word)
            if word in entity_set:
                score += 4.0 * importance  # Base score for exact match
                match_details[word] = True
            else:
                score -= 1.0  # Penalty for missing words
                match_details[word] = False

        # Bonus for sequential matches
        for i in range(len(shipment_tokens)):
            if i < len(entity_tokens) and shipment_tokens[i] == entity_tokens[i]:
                score += 2.0  # Bonus for words in same position

        # Penalize extra words in entity name
        extra_words = len(entity_set - shipment_set)
        score -= 0.5 * extra_words

        return score, match_details

def calculate_idf(token: str, total_docs: int) -> float:
    """Calculate inverse document frequency for a token."""
    token = token.upper()
    token_doc = index.find_one({"token": token})
    if token_doc:
        document_frequency = len(token_doc["entity_ids"])
        if total_docs > document_frequency:
            return math.log(total_docs / (1 + document_frequency))
        else:
            return 0.01  # Small value for very frequent tokens
    return 0

def find_best_matches(
    shipment: dict, total_docs: int, score_threshold: float = 1, top_n: int = 3
) -> List[dict]:
    jurisdiction_matcher = JurisdictionMatcher()
    word_matcher = WordMatcher()

    shipment_name = (shipment.get("trademo_name") or "").upper() if isinstance(shipment.get("trademo_name"), str) else str(shipment.get("trademo_name", ""))
    shipment_tokens = word_matcher.tokenize(shipment_name)
    shipment_normalized_name = (shipment.get("normalized_name") or "").upper() if isinstance(shipment.get("normalized_name"), str) else str(shipment.get("normalized_name", ""))
    shipment_country = shipment.get("country") or ""

    normalized_shipment_jurisdiction = jurisdiction_matcher.normalize_jurisdiction(
        shipment_country
    )

    # Initial scoring
    candidates = defaultdict(lambda: {"score": 0.0, "details": {}})

    # Token matching phase
    for token in shipment_tokens:
        idf = calculate_idf(token, total_docs)
        token_doc = index.find_one({"token": token})

        if token_doc:
            for entity_id in token_doc["entity_ids"]:
                if isinstance(entity_id, str):
                    entity_id = ObjectId(entity_id)
                candidates[entity_id]["score"] += idf

    # Detailed scoring phase
    for entity_id in list(candidates.keys()):
        entity = entities.find_one({"_id": entity_id})
        if not entity:
            continue

        entity_name = entity.get("name", "").upper()
        entity_tokens = word_matcher.tokenize(entity_name)

        # Word matching score
        word_score, match_details = word_matcher.calculate_word_match_score(
            shipment_tokens, entity_tokens
        )
        candidates[entity_id]["score"] += word_score
        candidates[entity_id]["word_matches"] = match_details

        # Jurisdiction scoring
        jurisdiction_score = jurisdiction_matcher.calculate_jurisdiction_score(
            shipment_country, entity.get("jurisdiction"), candidates[entity_id]["score"]
        )
        candidates[entity_id]["score"] += jurisdiction_score

        # Perfect match bonus
        if (shipment_normalized_name == entity.get("normalized_name", "").upper() and 
            normalized_shipment_jurisdiction == entity.get("jurisdiction")):
            candidates[entity_id]["score"] += 15  # Increased perfect match bonus

    # Filter and rank results
    filtered_candidates = [
        (entity_id, data)
        for entity_id, data in candidates.items()
        if data["score"] >= score_threshold
    ]

    sorted_candidates = sorted(
        filtered_candidates, key=lambda x: x[1]["score"], reverse=True
    )[:top_n]

    # Prepare final results
    matches = []
    for entity_id, data in sorted_candidates:
        entity = entities.find_one({"_id": entity_id})
        if entity:
            entity_name = entity.get("name", "Unknown")
            company_number = entity.get("company_number", "")
            jurisdiction_code = entity.get("jurisdiction_code", "Unknown")
            jurisdiction = jurisdiction_code.upper()

            matches.append({
                "entity_name": entity_name,
                "company_number": company_number,
                "jurisdiction": jurisdiction,
                "score": data["score"],
                "matched_jurisdiction": normalized_shipment_jurisdiction == jurisdiction,
            })

    return matches

def load_checkpoint() -> Optional[ObjectId]:
    """Load the last processed batch number from checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            last_id = checkpoint.get('last_processed_id')
            return ObjectId(last_id) if last_id else None
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, Exception) as e:
        console.print(f"[red]Error reading checkpoint file: {e}[/red]")
        console.print("Starting from beginning...")
        return None

def save_checkpoint(last_id: ObjectId):
    """Save the last processed batch number to checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({'last_processed_id': str(last_id)}, f)
    except Exception as e:
        console.print(f"[red]Error saving checkpoint: {e}[/red]")
        console.print(f"Last processed ID was: {last_id}")

def process_batch(start_id: Optional[ObjectId] = None, batch_size: int = BATCH_SIZE) -> Tuple[List[dict], List[tuple], Optional[ObjectId]]:
    """Process a batch of shipments and return matches, no_matches, and last processed ID."""
    total_docs = entities.estimated_document_count()
    matches = []
    no_matches = []
    last_id = None
    
    # Build query
    query = {}
    if start_id:
        query['_id'] = {'$gt': start_id}
    
    # Get batch of shipments
    batch = shipments.find(query).limit(batch_size)
    
    for shipment in track(batch, description=f"Processing batch (start_id: {start_id})...", total=batch_size):
        last_id = shipment['_id']
        shipment_name = str(shipment.get("trademo_name", "Unknown"))
        shipment_country = str(shipment.get("country", "Unknown"))

        found_matches = find_best_matches(shipment, total_docs, score_threshold=1)
        
        if found_matches:
            matches.append({
                'shipment_name': shipment_name,
                'shipment_country': shipment_country,
                'matches': found_matches
            })
        else:
            no_matches.append((shipment_name, shipment_country))
    
    return matches, no_matches, last_id

def display_batch_results(matches: List[dict], no_matches: List[tuple]):
    """Display results for a single batch, showing alternatives only if within 5 points of best match."""
    # Create the matches table
    matches_table = Table(title="Entity Matches")
    matches_table.add_column("Shipment Name", style="cyan")
    matches_table.add_column("Shipment Country", style="yellow")
    matches_table.add_column("Match", style="green")
    matches_table.add_column("Score", justify="right", style="blue")
    matches_table.add_column("Company Number", style="magenta")
    matches_table.add_column("Jurisdiction", style="yellow")
    
    # Add matches to table
    for match_group in matches:
        # Add best match as main row
        best_match = match_group['matches'][0]
        best_score = best_match['score']
        
        matches_table.add_row(
            str(match_group['shipment_name']),
            str(match_group['shipment_country']),
            str(best_match['entity_name']),
            f"{best_score:.1f}",
            str(best_match['company_number']),
            f"{best_match['jurisdiction']}{'✓' if best_match['matched_jurisdiction'] else '✗'}"
        )
        
        # Add alternate matches only if within 5 points of the best score
        for match in match_group['matches'][1:]:
            if best_score - match['score'] <= 5:
                matches_table.add_row(
                    "",  # Empty shipment name for alternates
                    "",  # Empty shipment country for alternates
                    f"└─ {str(match['entity_name'])}",
                    f"{match['score']:.1f}",
                    str(match['company_number']),
                    f"{match['jurisdiction']}{'✓' if match['matched_jurisdiction'] else '✗'}"
                )
    
    # Print matches table
    if matches:
        console.print("\nMatched Entities:")
        console.print(matches_table)
    
    # Print unmatched shipments
    if no_matches:
        no_matches_table = Table(title="\nUnmatched Shipments")
        no_matches_table.add_column("Shipment Name", style="red")
        no_matches_table.add_column("Country", style="yellow")
        
        for name, country in no_matches:
            no_matches_table.add_row(str(name), str(country))
        
        console.print(no_matches_table)
    
    # Print batch summary
    total_processed = len(no_matches) + len(matches)
    console.print(f"\nBatch Summary:")
    console.print(f"Total processed: {total_processed}")
    console.print(f"Matched: {len(matches)}")
    console.print(f"Unmatched: {len(no_matches)}")

def main():
    total_count = shipments.estimated_document_count()
    console.print(f"Total documents to process: {total_count}")
    
    # Load checkpoint if exists
    last_processed_id = load_checkpoint()
    if last_processed_id:
        console.print(f"Resuming from checkpoint after ID: {last_processed_id}")
    
    try:
        while True:
            # Process batch
            matches, no_matches, last_id = process_batch(last_processed_id, BATCH_SIZE)
            
            # Display results for this batch
            display_batch_results(matches, no_matches)
            
            # Save checkpoint
            if last_id:
                save_checkpoint(last_id)
                last_processed_id = last_id
            
            # Check if we've processed everything
            if not matches and not no_matches:
                console.print("Processing complete - no more documents to process.")
                break
                
            console.print("\nPress Enter to process next batch (Ctrl+C to exit)")
            input()
            
    except KeyboardInterrupt:
        console.print("\nProcessing interrupted. Progress saved in checkpoint file.")
        if last_id:
            save_checkpoint(last_id)
        console.print(f"Resume from ID: {last_id}")

if __name__ == "__main__":
    main()
    