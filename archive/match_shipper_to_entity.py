import re
from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
from typing import List, Dict, Set
from jurisdiction_neighborhood import (
    get_regional_jurisdictions_by_country,
    get_iso_code_by_country,
)

# MongoDB connection
client = MongoClient("mongodb://172.17.0.4:27017")
db = client["tradeverifyd"]


# shipments_cfg = {
#     "collection": "trademo_entities",
#     "jurisdiction": "country",
#     "name": "trademo_name",
#     "normalized_name": "normalized_name",
#     "tokenized_name": "tokenized_name",
# }

shipments_cfg = {
    "collection": "trademo_sourced_entities",
    "jurisdiction": "jurisdiction",
    "name": "name",
    "normalized_name": "name",
    "tokenized_name": "tokenized_name",
}


# entity_cfg = {
#     "collection": "opencorporates_entities",
#     "jurisdiction": "jurisdiction_code",
#     "name": "name",
#     "normalized_name": "normalised_name",
#     "entity_uid": "company_number",
#     "tokenized_name": "tokenized_name",
# }
entity_cfg = {
    "collection": "mesur.io_entities_notrademo",
    "jurisdiction": "jurisdiction",
    "name": "name",
    "normalized_name": "name",
    "entity_uid": "_id",
    "tokenized_name": "tokenized_name",
}

# Scoring and matching configuration
matching_cfg = {
    # Token preparation
    "min_token_length": 3,
    "stopwords": [
        "VARIABLE",
        "SOCIEDAD",
        "CAPITAL",
        "ANONIMA",
        "LIMITED",
        "LIABILITY",
        "COMPANY",
    ],
    # Scoring weights
    "name_similarity_weight": 0.7,
    "jurisdiction_weight": 0.3,
    # Jurisdiction scoring
    "exact_jurisdiction_score": 1.0,
    "neighboring_jurisdiction_score": 0.5,
    "non_matching_jurisdiction_score": 0.0,
    # Matching thresholds
    "min_score_threshold": 0.6,
    "max_search_results": 20,
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
    return re.sub(r"[^\w\s]", " ", name).strip()


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

    # all tokens
    # return tokens

    # only substantive tokens
    tokens = [word for word in tokens if len(word) >= matching_cfg["min_token_length"]]

    # filtered tokens
    tokens = [token for token in tokens if token not in matching_cfg["stopwords"]]

    # only first three tokens
    # tokens = tokens[:min(3, len(tokens))]

    return tokens


def build_and_query(tokens: List[str]) -> Dict:
    """Build MongoDB $and query for tokenized name search."""
    if not tokens:
        return {}
    return {"tokenized_name": {"$all": tokens}}


def calculate_match_score(
    trademo_tokens: set[str],
    entity_tokens: set[str],
    shipper_jurisdiction: str,
    entity_jurisdiction: str,
    regional_jurisdictions: list[str],
) -> float:
    """
    Calculate similarity score between shipping entity and registered entity.

    The final score (0-1) is weighted:
    - Name similarity: 70% of final score
    - Jurisdiction match: 30% of final score

    Jurisdiction scoring:
    - Exact jurisdiction match = 1.0
    - Neighboring jurisdiction = 0.3
    - Non-matching jurisdiction = 0.0

    Args:
        trademo_tokens: Set of tokens from shipping entity name
        entity_tokens: Set of tokens from registered entity name
        shipper_jurisdiction: Country/jurisdiction code of shipping entity
        entity_jurisdiction: Country/jurisdiction code of registered entity
        regional_jurisdictions: List of neighboring jurisdiction codes

    Returns:
        float: Combined similarity score between 0 and 1
    """
    # Avoid division by zero for empty token sets
    if not trademo_tokens or not entity_tokens:
        return 0.0

    # Calculate name similarity score (intersection over union)
    intersection = len(trademo_tokens & entity_tokens)
    union = len(trademo_tokens | entity_tokens)
    name_score = intersection / union if union > 0 else 0.0

    shipper_code = (
        shipper_jurisdiction.upper()
        if len(shipper_jurisdiction) == 2
        else get_iso_code_by_country(shipper_jurisdiction)
    )
    entity_main_jurisdiction = entity_jurisdiction.upper().split("_")[0]

    # Calculate jurisdiction score
    jurisdiction_score = matching_cfg["non_matching_jurisdiction_score"]
    if entity_main_jurisdiction.upper() == shipper_code.upper():
        jurisdiction_score = matching_cfg["exact_jurisdiction_score"]
    elif entity_main_jurisdiction.upper() in [
        j.upper() for j in regional_jurisdictions
    ]:
        jurisdiction_score = matching_cfg["neighboring_jurisdiction_score"]

    # Calculate final weighted score
    final_score = (name_score * matching_cfg["name_similarity_weight"]) + (
        jurisdiction_score * matching_cfg["jurisdiction_weight"]
    )

    return final_score


def find_matches(
    trademo_tokens: set[str],
    shipping_country: str,
    score_threshold: float = matching_cfg["min_score_threshold"],
) -> list[dict]:
    """
    Find matching entities for a shipping company using enhanced scoring system.
    Returns only perfect matches (score=1.0) if found, otherwise returns all matches above threshold.

    Matches are found using a combination of name similarity and jurisdiction proximity.
    The scoring system weights name similarity at 70% and jurisdiction matching at 30%.

    Args:
        trademo_tokens: Set of tokens from the shipping company name
        shipping_country: Country name or ISO code where the shipping company operates
        score_threshold: Minimum score required for a match (default: 0.5)

    Returns:
        list[dict]: List of matching entities with their scores.
                   If a perfect match (1.0) exists, returns only perfect matches.
    """
    tokens = prepare_tokens_for_search(trademo_tokens)
    if not tokens:
        return []

    matches = []
    perfect_matches = []
    query = build_and_query(tokens)

    # Get potential jurisdictions for geographic matching
    regional_jurisdictions = get_regional_jurisdictions_by_country(shipping_country)

    # Convert shipper country name to ISO code if it's not already a code
    shipper_code = (
        shipping_country.upper()
        if len(shipping_country) == 2
        else get_iso_code_by_country(shipping_country)
    )

    # Search for potential matches
    for entity in (
        entities.find(query)
        .limit(matching_cfg["max_search_results"])
        .hint("tokenized_name_1")
    ):
        entity_tokens = set(entity.get(ENTITY_TOKENIZED_NAME, []))
        entity_jurisdiction = entity.get(ENTITY_JURISDICTION, "")

        # Extract main country code from entity jurisdiction if it contains a subdivision
        entity_main_jurisdiction = entity_jurisdiction.upper().split("_")[0]

        # Calculate name similarity score (intersection over union)
        intersection = len(trademo_tokens & entity_tokens)
        union = len(trademo_tokens | entity_tokens)
        name_score = intersection / union if union > 0 else 0.0

        # Calculate jurisdiction score
        jurisdiction_score = 0.0
        if shipper_code and entity_main_jurisdiction == shipper_code:
            jurisdiction_score = 1.0
        elif entity_main_jurisdiction in [j.upper() for j in regional_jurisdictions]:
            jurisdiction_score = 0.3

        # Calculate final weighted score
        score = (name_score * matching_cfg["name_similarity_weight"]) + (
            jurisdiction_score * matching_cfg["jurisdiction_weight"]
        )
        # Only include matches that meet the threshold
        if score >= score_threshold:
            match_entry = {
                "entity_name": entity.get(ENTITY_NAME, ""),
                "normalized_name": entity.get(ENTITY_NORMALIZED_NAME, ""),
                "jurisdiction": entity_jurisdiction,
                "company_number": str(entity.get(ENTITY_UID, "")),
                "score": score,
            }

            if score == 1.0:
                perfect_matches.append(match_entry)
            else:
                matches.append(match_entry)

    # If we found any perfect matches, return only those
    if perfect_matches:
        return perfect_matches

    # Otherwise, return all matches sorted by score
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches


def display_matches(matches: List[Dict]):
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
            match["company_number"],
        )

    console.print(table)


def main():
    for shipment in shipments.find():
        trademo_name = shipment.get(SHIPMENTS_NAME, "")

        # normalized_name = shipment.get(SHIPMENTS_NORMALIZED_NAME,"")
        # shipment_tokenized_name = set(shipment.get(SHIPMENTS_TOKENIZED_NAME,""))
        # did not expand industry terms in opencorporates so use the shipment name here (not norm)
        shipment_name = shipment.get(SHIPMENTS_NAME, "")
        shipment_tokenized_name = set(tokenize_name(clean_name(shipment_name)))

        shipper_country = shipment.get(SHIPMENTS_JURISDICTION, "")
        console.print(
            f"\n[bold red]Processing: {trademo_name}: {shipper_country}[/bold red]"
        )

        matches = find_matches(shipment_tokenized_name, shipper_country)
        if matches:
            display_matches(matches)
        else:
            console.print("[yellow]No matches found[/yellow]")
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()
