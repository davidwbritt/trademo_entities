import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

def process_nested_fields(df: pd.DataFrame, prefix: str = 'registered_address') -> pd.DataFrame:
    """Process fields with dot notation into nested dictionaries using vectorized operations."""
    prefix_cols = [col for col in df.columns if col.startswith(f"{prefix}.")]
    non_prefix_cols = [col for col in df.columns if not col.startswith(f"{prefix}.")]
    
    # Create new dataframe without prefix columns
    new_df = df[non_prefix_cols].copy()
    
    if not prefix_cols:
        new_df[prefix] = None
        return new_df
    
    # Extract field names once
    field_names = {col: col.split('.', 1)[1] for col in prefix_cols}
    
    # Create nested dictionaries using dictionary comprehension
    nested_data = df[prefix_cols].apply(
        lambda row: {
            field_names[col]: val 
            for col, val in row.items() 
            if pd.notna(val)
        } or None,
        axis=1
    )
    
    new_df[prefix] = nested_data
    return new_df

def load_csvs_to_mongodb(directory: str, db_name: str, collection_name: str, mongo_uri: str = "mongodb://localhost:27017", chunk_size: int = 10000) -> None:
    """Load CSV files into MongoDB, handling nested fields and NaN values."""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    csv_files = sorted([f for f in os.listdir(directory) if f.endswith(".csv")])
    if not csv_files:
        raise ValueError(f"No CSV files found in directory: {directory}")

    for csv_file in tqdm(csv_files, desc="Loading CSV files"):
        csv_path = os.path.join(directory, csv_file)
        
        try:
            # Process file in chunks to reduce memory usage
            for chunk in pd.read_csv(csv_path, low_memory=False, chunksize=chunk_size):
                # Process nested fields
                chunk = process_nested_fields(chunk)
                
                # Replace NaN values with None
                chunk = chunk.where(pd.notnull(chunk), None)

                # Convert to records and handle any remaining NaN values
                records = chunk.to_dict(orient="records")
                if records:
                    # Handle any remaining float NaN values
                    for record in records:
                        for key, value in record.items():
                            if isinstance(value, float) and pd.isna(value):
                                record[key] = None

                    try:
                        collection.insert_many(records, ordered=False)
                    except Exception as e:
                        print(f"Error inserting records from {csv_file}: {e}")

        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            continue

    print(f"Finished loading all CSV files into MongoDB collection '{collection_name}'.")

if __name__ == "__main__":
    directory = "companies_parsed"
    db_name = "tradeverifyd"
    collection_name = "opencorporates_raw"
    mongo_uri = "mongodb://172.17.0.4:27017"

    load_csvs_to_mongodb(directory, db_name, collection_name, mongo_uri)