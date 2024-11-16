from pymongo import MongoClient
from rich.console import Console
from rich.table import Table
import re
from typing import List, Dict, Set

# MongoDB connection
client = MongoClient("mongodb://172.17.0.4:27017")
db = client["tradeverifyd"]
shipments = db["trademo_entities"]
entities = db["opencorporates_entities"]
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
    return {word.lower() for word in cleaned.split()}

def calculate_match_score(trademo_tokens: Set[str], entity_tokens: Set[str]) -> float:
    """Calculate similarity score between two sets of tokens."""
    if not trademo_tokens or not entity_tokens:
        return 0.0
        
    intersection = len(trademo_tokens & entity_tokens)
    union = len(trademo_tokens | entity_tokens)
    
    return intersection / union if union > 0 else 0.0

def build_regex_pattern(name: str) -> str:
    """Build regex pattern from first few words."""
    if not isinstance(name, str):
        return ""
    
    name = clean_name(name)
    tokens = [word.strip() for word in name.split() if word.strip()]
    if not tokens:
        return ""
    pattern = r"^" + r"\s+".join(map(re.escape, tokens[:min(3, len(tokens))]))
    return pattern

def find_matches(trademo_name: str) -> List[Dict]:
    pattern = build_regex_pattern(trademo_name)
    if not pattern:
        return []
    
    matches = []
    regex_query = {"normalised_name": {"$regex": pattern}}
    trademo_tokens = tokenize_name(trademo_name)
    
    for entity in entities.find(regex_query).limit(30).hint('normalised_name_1'):
        entity_tokens = tokenize_name(entity.get("normalised_name", ""))
        score = calculate_match_score(trademo_tokens, entity_tokens)
        
        matches.append({
            "entity_name": entity.get("name", ""),
            "normalised_name": entity.get("normalised_name", ""),
            "jurisdiction": entity.get("jurisdiction_code", ""),
            "company_number": str(entity.get("company_number", "")),
            "score": score
        })
    
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches

def display_matches(trademo_name: str, shipper_country: str, matches: List[Dict]):
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Score", style="cyan", justify="right")
    table.add_column("Matched Entity", style="green")
    table.add_column("Normalised Name", style="yellow")
    table.add_column("Jurisdiction", style="blue")
    table.add_column("Company Number", style="white")
    
    for match in matches:
        table.add_row(
            f"{match['score']:.3f}",
            match["entity_name"],
            match["normalised_name"],
            match["jurisdiction"],
            match["company_number"]
        )
    
    console.print(table)

def main():
    for shipment in shipments.find().limit(100):
        trademo_name = shipment.get("trademo_name", "")
        shipper_country = shipment.get("country", "")
        console.print(f"\n[bold red]Processing: {trademo_name}: {shipper_country}[/bold red]")
        
        matches = find_matches(trademo_name)
        if matches:
            display_matches(trademo_name, shipper_country, matches)
        else:
            console.print("[yellow]No matches found[/yellow]")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":

    main()