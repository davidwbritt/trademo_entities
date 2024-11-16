from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
from typing import List, Dict, Set
import re
# MongoDB connection
client = MongoClient("mongodb://172.17.0.4:27017")
db = client["tradeverifyd"]
#shipments = db["trademo_entities"]

shipments_cfg = {
            "collection": "trademo_entities",
            "jurisdiction": "country",
            "name":"trademo_name",
            "normalized_name": "normalized_name",
            "tokenized_name":"tokenized_name"
        }

entity_cfg = {
            "collection": "opencorporates_entities",
            "jurisdiction": "jurisdiction_code",
            "name": "name",
            "normalized_name": "normalised_name",
            "entity_uid": "company_number",
            "tokenized_name":"tokenized_name"
        }

shipments = db[shipments_cfg["collection"]]
SHIPMENTS_NAME = shipments_cfg["name"]
SHIPMENTS_NORMALIZED_NAME = shipments_cfg["normalized_name"]
SHIPMENTS_JURISDICTION = shipments_cfg["jurisdiction"]
SHIPMENTS_TOKENIZED_NAME = shipments_cfg["tokenized_name"]


entities = db[entity_cfg["collection"]]
ENTITY_JURISDICTION = entity_cfg["jurisdiction"]
ENTITY_NAME = entity_cfg["name"]
ENTITY_NORMALIZED_NAME = entity_cfg["normalized_name"]
ENTITY_UID = entity_cfg["entity_uid"]
ENTITY_TOKENIZED_NAME = entity_cfg["tokenized_name"]


console = Console()

def clean_name(name: str) -> str:
    """Remove punctuation and extra whitespace."""
    if not isinstance(name, str):
        return ""
    return re.sub(r'[^\w\s]', ' ', name).strip()

def tokenize_name(name: str) -> Set[str]:
    """Convert name to lowercase tokens."""
    if not isinstance(name, str):
        return set()
    cleaned = clean_name(name)
    return {word.upper() for word in cleaned.split()}


def prepare_tokens_for_search(tokens: Set[str]) -> Set[str]:
    """Get tokens from tokenized name."""
    if not tokens or tokens == []:
        return []

    #all tokens
    #return tokens

    #only substantive tokens
    tokens = [word for word in tokens if len(word) >= 3]

    #filtered tokens
    stopwords = ["VARIABLE", "SOCIEDAD", "CAPITAL", "ANONIMA", "LIMITED", "LIABILITY", "COMPANY"]
    tokens = [token for token in tokens if token not in stopwords]

    #only first three tokens
    #tokens = tokens[:min(3, len(tokens))]

    return tokens

def build_and_query(tokens: List[str]) -> Dict:
    """Build MongoDB $and query for tokenized name search."""
    if not tokens:
        return {}
    return {
        "tokenized_name": {
            "$all": tokens
        }
    }

def calculate_match_score(trademo_tokens: Set[str], entity_tokens: Set[str]) -> float:
    """Calculate similarity score between two sets of tokens."""
    if not trademo_tokens or not entity_tokens:
        return 0.0
    
    intersection = len(trademo_tokens & entity_tokens)
    union = len(trademo_tokens | entity_tokens)
    
    return intersection / union if union > 0 else 0.0

def find_matches(trademo_tokens: Set[str]) -> List[Dict]:
    tokens = prepare_tokens_for_search(trademo_tokens)
    if not tokens:
        return []
    
    matches = []
    query = build_and_query(tokens)


    for entity in entities.find(query).limit(30).hint('tokenized_name_1'):
        entity_tokens = set(entity.get(ENTITY_TOKENIZED_NAME, []))  # Changed from split() to handle list
        score = calculate_match_score(trademo_tokens, entity_tokens)
        matches.append({
            "entity_name": entity.get(ENTITY_NAME, ""),
            "normalized_name": entity.get(ENTITY_NORMALIZED_NAME, ""),
            "jurisdiction": entity.get(ENTITY_JURISDICTION, ""),
            "company_number": str(entity.get(ENTITY_UID, "")),
            "score": score
        })
   
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches

def display_matches(trademo_name: str, shipper_country: str, matches: List[Dict]):
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Score", style="cyan", justify="right")
    table.add_column("Matched Entity", style="green")
    table.add_column("Normalized Name", style="yellow")
    table.add_column("Jurisdiction", style="blue")
    table.add_column("Company Number", style="white")
    
    for match in matches:
        table.add_row(
            f"{match['score']:.3f}",
            match["entity_name"],
            match["normalized_name"],
            match["jurisdiction"],
            match["company_number"]
        )
    
    console.print(table)

def main():
    for shipment in shipments.find().limit(100):
        trademo_name = shipment.get(SHIPMENTS_NAME, "")

        #normalized_name = shipment.get(SHIPMENTS_NORMALIZED_NAME,"")
        #shipment_tokenized_name = set(shipment.get(SHIPMENTS_TOKENIZED_NAME,""))
        #did not expand industry terms in opencorporates so use the shipment name here (not norm)
        shipment_name = shipment.get(SHIPMENTS_NAME,"")
        shipment_tokenized_name = set(tokenize_name(clean_name(shipment_name)))

        shipper_country = shipment.get(SHIPMENTS_JURISDICTION, "")
        console.print(f"\n[bold red]Processing: {trademo_name}: {shipper_country}[/bold red]")

        matches = find_matches(shipment_tokenized_name)
        if matches:
            display_matches(trademo_name, shipper_country, matches)
        else:
            console.print("[yellow]No matches found[/yellow]")
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()