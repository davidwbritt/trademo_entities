import os
import time
from collections import defaultdict

from bson import ObjectId  # Import ObjectId to handle MongoDB _id correctly
from pymongo import MongoClient, errors

# MongoDB connection parameters
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"

client = MongoClient(MONGO_URI, socketTimeoutMS=120000, serverSelectionTimeoutMS=120000)
db = client[DATABASE]
entities_collection = db["opencorporates_entities"]  # Collection to index
index_collection = db["entity_token_index"]  # Collection for inverted index

# File for checkpointing
last_id_checkpoint = "last_id_checkpoint.txt"

# Determine starting point based on checkpoint
if os.path.exists(last_id_checkpoint):
    # Load last checkpoint if it exists
    with open(last_id_checkpoint, "r") as f:
        last_id_str = f.read().strip()
        last_id = (
            ObjectId(last_id_str) if last_id_str else None
        )  # Convert to ObjectId if non-empty
    print(f"Resuming from last_id: {last_id}")
else:
    # If checkpoint file doesn't exist, drop the index collection to start fresh
    print(
        "Checkpoint file not found. Dropping the index collection and starting from scratch."
    )
    index_collection.drop()
    last_id = None  # Start from the beginning

# Parameters for batch processing and retry logic
batch_size = 50000  # Number of documents to process per batch
max_retries = 3  # Number of retries per batch in case of errors

# Main processing loop
while True:
    filter_criteria = {"_id": {"$gt": last_id}} if last_id else {}

    # Retry loop for each batch
    for attempt in range(max_retries):
        try:
            # Fetch the batch of documents from MongoDB
            batch = list(
                entities_collection.find(
                    filter_criteria, {"_id": 1, "tokenized_name": 1}
                )
                .sort("_id")
                .limit(batch_size)
            )
            break  # Exit retry loop on successful retrieval
        except errors.OperationFailure as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt + 1 == max_retries:
                raise  # If max retries reached, re-raise the error
            time.sleep(5)  # Wait a few seconds before retrying

    if not batch:
        print("No more documents to process.")
        break  # Stop if there are no more documents

    # Update last_id to the last document's _id in this batch
    last_id = batch[-1]["_id"]

    # Temporary dictionary to build the inverted index for the batch
    inverted_index = defaultdict(list)

    # Process each document in the batch
    for doc in batch:
        entity_id = doc["_id"]
        tokens = doc.get("tokenized_name", [])

        # Populate the inverted index with tokens and entity IDs
        for token in tokens:
            inverted_index[token].append(entity_id)

    # Insert the inverted index data into MongoDB in chunks
    bulk_insert = []
    for token, entity_ids in inverted_index.items():
        # Chunk large lists to avoid BSON size limits
        for i in range(
            0, len(entity_ids), 1000
        ):  # Chunk size of 1000 to stay well below 16MB limit
            bulk_insert.append(
                {
                    "token": token,
                    "chunk": i // 1000,  # Assign a chunk number
                    "entity_ids": entity_ids[i : i + 1000],
                }
            )

    # Only perform the insert if there’s data in bulk_insert
    if bulk_insert:
        for attempt in range(max_retries):
            try:
                index_collection.insert_many(bulk_insert)
                print(f"Processed batch up to _id: {last_id}")
                break  # Exit loop if insertion is successful
            except errors.PyMongoError as e:
                print(f"Insert attempt {attempt + 1} failed: {e}")
                if attempt + 1 == max_retries:
                    raise  # If max retries reached, re-raise the error
                time.sleep(30)  # Wait a few seconds before retrying

    # Save the last_id checkpoint after each processed batch, writing it as a string
    with open(last_id_checkpoint, "w") as f:
        f.write(str(last_id))

print("Inverted index creation completed.")
