import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from bson import ObjectId
from pymongo import MongoClient, errors

# MongoDB connection parameters
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"

client = MongoClient(MONGO_URI, socketTimeoutMS=3 * 60 * 60 * 1000, serverSelectionTimeoutMS=3 * 60 * 60 * 1000)
db = client[DATABASE]
source_collection = db["entity_token_index"]
target_collection = db["entity_token_index_filtered"]

BATCH_SIZE = 1000
MAX_RETRIES = 3
CHECKPOINT_FILE = "token_consolidation_checkpoint.txt"


def load_checkpoint() -> Tuple[ObjectId, bool]:
    """
    Load the last processed ObjectId from checkpoint file
    Returns (last_id, is_new_start)
    """
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                last_id_str = f.read().strip()
                if last_id_str:
                    last_id = ObjectId(last_id_str)
                    print(f"Resuming from _id: {last_id}")
                    return last_id, False
                return None, False
    except (IOError, OSError) as e:
        print(f"Error reading checkpoint file: {e}")
        return None, True

    print("Checkpoint file not found. Starting from scratch.")
    return None, True


def save_checkpoint(last_id: str):
    """
    Save the last processed ObjectId to checkpoint file
    """
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(last_id))
            print(f"Saved checkpoint: {last_id}")
    except (IOError, OSError) as e:
        print(f"Warning: Could not save checkpoint: {e}")


def identify_tokens_with_large_chunks():
    """
    Identify tokens with any chunk >= 1
    Returns a set of tokens to exclude
    """
    print("Identifying tokens with large chunks...")

    # Step 1: Identify tokens with chunk >= 1
    pipeline = [
        {"$match": {"chunk": {"$gte": 1}}},
        {"$group": {"_id": "$token"}}
    ]

    try:
        # Retrieve tokens with chunk >= 1
        tokens_to_exclude = {doc["_id"] for doc in source_collection.aggregate(pipeline)}
        print(f"Identified {len(tokens_to_exclude)} tokens to exclude.")
        return tokens_to_exclude
    except errors.PyMongoError as e:
        print(f"Error during token identification: {e}")
        raise


def process_tokens_batch(tokens_to_exclude, last_id: str = None) -> Tuple[List[Dict[str, Any]], str]:
    """
    Process a batch of documents, excluding any tokens that are in the tokens_to_exclude set
    Returns tuple of (processed_documents, last_processed_id)
    """
    # Convert last_id to ObjectId if it's a string
    last_id_obj = ObjectId(last_id) if isinstance(last_id, str) and last_id else last_id

    # Get batch of documents and filter out excluded tokens
    query = {
        "_id": {"$gt": last_id_obj} if last_id_obj else {"$exists": True},
        "token": {"$nin": list(tokens_to_exclude)}
    }

    try:
        # Get a batch of documents, sorted by _id
        docs = list(source_collection.find(query).sort("_id", 1).limit(BATCH_SIZE))
    except errors.OperationFailure as e:
        print(f"Error fetching document batch: {e}")
        raise

    if not docs:
        return [], None

    result_docs = []
    token_groups = defaultdict(list)

    # Group documents by token
    for doc in docs:
        token_groups[doc["token"]].append(doc)

    # Merge entity_ids for each token group
    for token, token_docs in token_groups.items():
        # Create a set of ObjectIds for deduplication
        all_entity_ids = set()
        for doc in token_docs:
            # Handle each entity_id in the array
            for entity_id in doc["entity_ids"]:
                # If it's already an ObjectId, add it directly
                if isinstance(entity_id, ObjectId):
                    all_entity_ids.add(entity_id)
                # If it's a dict with $oid, convert to ObjectId
                elif isinstance(entity_id, dict) and "$oid" in entity_id:
                    all_entity_ids.add(ObjectId(entity_id["$oid"]))
                # If it's a string, convert to ObjectId
                else:
                    all_entity_ids.add(ObjectId(str(entity_id)))

        result_docs.append(
            {
                "_id": ObjectId(),  # Generate new _id for merged document
                "token": token,
                "chunk": 0,
                "entity_ids": list(all_entity_ids),  # Convert set back to list for MongoDB storage
            }
        )

    return result_docs, str(docs[-1]["_id"]) if docs else None


def main():
    # Load checkpoint and determine if we need to drop the collection
    last_id, is_new_start = load_checkpoint()

    if is_new_start:
        print("Dropping target collection for fresh start...")
        target_collection.drop()
        # Create initial checkpoint file
        save_checkpoint("")  # Empty string for initial checkpoint
        
        # Step to identify tokens with large chunks
        tokens_to_exclude = identify_tokens_with_large_chunks()
    else:
        print("Resuming previous operation, keeping existing target collection...")
        # Load previously identified tokens if not starting fresh
        tokens_to_exclude = identify_tokens_with_large_chunks()

    processed_count = 0
    error_count = 0

    while True:
        try:
            print(f"Processing batch after _id: {last_id}")
            batch_docs, new_last_id = process_tokens_batch(tokens_to_exclude, last_id)

            if not batch_docs:
                print("No more documents to process.")
                break

            # Insert the processed documents into the target collection
            target_collection.insert_many(batch_docs)
            processed_count += len(batch_docs)

            # Update last_id and save checkpoint
            last_id = new_last_id
            if last_id:  # Check for valid last_id before saving
                save_checkpoint(last_id)

            print(
                f"Successfully processed {len(batch_docs)} documents. Total processed: {processed_count}"
            )
            error_count = 0  # Reset error count on success

        except errors.PyMongoError as e:
            error_count += 1
            print(f"Error processing batch (attempt {error_count}): {e}")

            if error_count >= MAX_RETRIES:
                print("Max retry attempts reached. Exiting.")
                raise

            time.sleep(30)  # Wait before retrying
            continue

        except Exception as e:
            print(f"Unexpected error: {e}")
            raise

    print(f"Processing completed. Total documents processed: {processed_count}")

    # Create index if we processed any documents
    if processed_count > 0:
        print("Creating index on token field...")
        target_collection.create_index("token")
        print("Index created successfully.")


if __name__ == "__main__":
    main()
