import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
from typing import List, Dict, Any

def create_alias_documents(row: pd.Series) -> List[Dict[str, Any]]:
    """Create alias documents from a pandas Series (CSV row)."""
    aliases = []
    
    # Base information common across all aliases for this company
    base_info = {
        "jurisdiction": row['jurisdiction_code'],
        "company_number": int(row['company_number']),  # Ensure integer type
        "canonical_name": row['normalised_name']
    }
    
    # Add current name
    if pd.notna(row.get('name')):
        aliases.append({
            **base_info,
            "alias": row['name']
        })
    
    # Add normalized name
    if pd.notna(row.get('normalised_name')):
        aliases.append({
            **base_info,
            "alias": row['normalised_name']
        })
    
    # Add previous names
    if pd.notna(row.get('previous_names')):
        prev_names = (
            row['previous_names'].split('|') 
            if isinstance(row['previous_names'], str) and '|' in row['previous_names']
            else [row['previous_names']]
        )
        
        for prev_name in prev_names:
            if prev_name and pd.notna(prev_name):
                aliases.append({
                    **base_info,
                    "alias": prev_name.strip()
                })
    
    return aliases

def load_csvs_to_aliases(
    directory: str,
    db_name: str,
    collection_name: str,
    mongo_uri: str = "mongodb://localhost:27017",
    batch_size: int = 10000
) -> None:
    """
    Load required fields from CSV files directly into entity_aliases collection.
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]  # Use the passed collection name
    
    # Required fields only
    fields = ['company_number', 'jurisdiction_code', 'name', 'normalised_name', 'previous_names']
    
    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    if not csv_files:
        raise ValueError(f"No CSV files found in directory: {directory}")
    
    print(f"Starting fresh - dropping {collection_name} collection...")
    collection.drop()
    
    print("Creating indexes...")
    collection.create_index([
        ('alias', 1),
        ('jurisdiction', 1)
    ], unique=False)
    collection.create_index('company_number')
    
    total_aliases = 0
    buffer = []
    
    # Process each CSV file
    for csv_file in tqdm(csv_files, desc="Processing CSV files"):
        csv_path = os.path.join(directory, csv_file)
        
        try:
            # Read only required fields from CSV
            for chunk in pd.read_csv(
                csv_path,
                usecols=fields,
                dtype={
                    'company_number': int,
                    'jurisdiction_code': str,
                    'name': str,
                    'normalised_name': str,
                    'previous_names': str
                },
                chunksize=batch_size,
                low_memory=False
            ):
                # Process each row in the chunk
                for _, row in chunk.iterrows():
                    aliases = create_alias_documents(row)
                    buffer.extend(aliases)
                    
                    # If buffer reaches batch size, insert and clear it
                    if len(buffer) >= batch_size:
                        try:
                            collection.insert_many(buffer)
                            total_aliases += len(buffer)
                            buffer = []
                        except Exception as e:
                            print(f"\nError inserting batch: {e}")
                
        except Exception as e:
            print(f"\nError processing {csv_file}: {e}")
            continue
    
    # Insert any remaining documents in buffer
    if buffer:
        try:
            collection.insert_many(buffer)
            total_aliases += len(buffer)
        except Exception as e:
            print(f"\nError inserting final batch: {e}")
    
    print(f"\nFinished processing. Total aliases created: {total_aliases:,}")

if __name__ == "__main__":
    directory = "companies_parsed"  # This matches your folder name
    db_name = "tradeverifyd"
    collection_name = "entity_aliases"
    mongo_uri = "mongodb://172.17.0.4:27017"
    load_csvs_to_aliases(
        directory=directory,
        db_name=db_name,
        collection_name=collection_name,
        mongo_uri=mongo_uri,
        batch_size=10000
    )
    