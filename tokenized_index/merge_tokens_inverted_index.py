import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from bson import ObjectId
from pymongo import MongoClient, errors

# MongoDB connection parameters
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"

client = MongoClient(MONGO_URI, socketTimeoutMS=120000, serverSelectionTimeoutMS=120000)
db = client[DATABASE]
distinct_tokens = db["distinct_tokens"]
source_collection = db["entity_token_index_filtered"]
target_collection = db["entity_token_index_final"]

BATCH_SIZE = 5000
MAX_RETRIES = 3
CHECKPOINT_FILE = "platform/earthstream_services/src/earthstream_services/services/trademo_entity_resolution/token_merge_checkpoint.txt"


def load_checkpoint() -> Tuple[str, bool]:
    """
    Load the last processed token from checkpoint file
    Returns (last_token, is_new_start)
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            last_token = f.read().strip()
            if last_token:
                print(f"Resuming from token: {last_token}")
                return last_token, False
            return None, False

    print("Checkpoint file not found. Starting from scratch.")
    return None, True


def save_checkpoint(token: str):
    """
    Save the last processed token to checkpoint file
    """
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(token))
        print(f"Saved checkpoint: {token}")


def get_next_token_batch(last_token: str = None) -> List[Dict[str, Any]]:
    """
    Get next batch of tokens from distinct_tokens collection
    """
    query = {"_id": {"$gt": last_token}} if last_token else {}

    try:
        return list(distinct_tokens.find(query).sort("_id", 1).limit(BATCH_SIZE))
    except errors.OperationFailure as e:
        print(f"Error fetching token batch: {e}")
        raise


def process_token_batch(
    token_docs: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Process a batch of tokens all at once, skipping those with too many entities
    """
    # Extract all tokens from the batch
    tokens = [doc["_id"] for doc in token_docs]

    # Get all documents for these tokens in one query
    try:
        source_docs = list(
            source_collection.find(
                {"token": {"$in": tokens}}, {"token": 1, "entity_ids": 1}
            ).hint("token_1")
        )
    except errors.OperationFailure as e:
        print(f"Error fetching documents: {e}")
        raise

    # Group documents by token
    token_groups = defaultdict(list)
    for doc in source_docs:
        token_groups[doc["token"]].append(doc)

    # Process each token group
    merged_docs = []
    total_entities = 0
    skipped_count = 0

    for token in tokens:
        # Get all documents for this token
        docs = token_groups.get(token, [])

        if not docs:
            continue

        # First count total entities before merging
        total_entity_count = sum(len(doc["entity_ids"]) for doc in docs)

        # Skip if total would exceed limit
        if total_entity_count > 200:
            print(
                f"Skipping token '{token}' - too many entities ({total_entity_count})"
            )
            skipped_count += 1
            continue

        # Merge entity_ids from all documents
        all_entity_ids = set()
        for doc in docs:
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

        entity_ids_list = list(all_entity_ids)
        total_entities += len(entity_ids_list)

        merged_docs.append(
            {
                "_id": ObjectId(),
                "token": token,
                "chunk": 0,
                "entity_ids": entity_ids_list,
            }
        )

    if skipped_count > 0:
        print(f"Skipped {skipped_count} tokens in this batch due to entity count > 200")

    return merged_docs, total_entities


def main():
    # Load checkpoint and determine if we need to drop the collection
    last_id, is_new_start = load_checkpoint()

    if is_new_start:
        print("Dropping target collection for fresh start...")
        target_collection.drop()
        # Create initial checkpoint file
        save_checkpoint("")
    else:
        print("Resuming previous operation, keeping existing target collection...")

    processed_count = 0
    error_count = 0
    total_entities = 0
    start_time = time.time()

    while True:
        try:
            # Get next batch of tokens
            token_docs = get_next_token_batch(last_id)

            if not token_docs:
                print("No more tokens to process.")
                break

            # Process the entire batch at once
            merged_docs, batch_entities = process_token_batch(token_docs)

            # Insert merged documents if we have any
            if merged_docs:
                target_collection.insert_many(merged_docs)
                processed_count += len(merged_docs)
                total_entities += batch_entities  # type: ignore

            # Update last_id and save checkpoint
            last_id = token_docs[-1]["_id"]
            save_checkpoint(last_id)

            elapsed_time = time.time() - start_time
            docs_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0

            print(f"Batch processed: {len(merged_docs)} tokens. Running totals:")
            print(f"  - Tokens processed: {processed_count}")
            print(f"  - Total entities referenced: {total_entities}")
            print(
                f"  - Average entities per token: {total_entities/processed_count if processed_count > 0 else 0:.2f}"
            )
            print(f"  - Processing rate: {docs_per_second:.2f} tokens/second")
            print(f"  - Elapsed time: {elapsed_time/60:.2f} minutes")
            error_count = 0

        except errors.PyMongoError as e:
            error_count += 1
            print(f"Error processing batch (attempt {error_count}): {e}")

            if error_count >= MAX_RETRIES:
                print("Max retry attempts reached. Exiting.")
                raise

            time.sleep(30)
            continue

        except Exception as e:
            print(f"Unexpected error: {e}")
            raise

    total_time = time.time() - start_time
    print("\nProcessing completed!")
    print("Final statistics:")
    print(f"  - Total tokens processed: {processed_count}")
    print(f"  - Total entity references: {total_entities}")
    print(
        f"  - Average entities per token: {total_entities/processed_count if processed_count > 0 else 0:.2f}"
    )
    print(f"  - Total processing time: {total_time/60:.2f} minutes")
    print(
        f"  - Overall processing rate: {processed_count/total_time:.2f} tokens/second"
    )

    # Create index if we processed any documents
    if processed_count > 0:
        print("\nCreating index on token field...")
        target_collection.create_index("token")
        print("Index created successfully.")


if __name__ == "__main__":
    main()
