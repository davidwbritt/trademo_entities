from typing import Dict, Any
import logging
from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from tqdm import tqdm

from config import MONGODB_URI, DB_NAME, SHIPMENTS_CFG, ENTITY_CFG, MATCHING_CFG
from entity_matcher import EntityMatcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"entity_matching_{datetime.now():%Y%m%d_%H%M%S}.log"),
        logging.StreamHandler(),
    ],
)


def process_shipments_batch(
    matcher: EntityMatcher,
    shipments_collection: Collection,
    batch_docs: list[Dict[str, Any]],
) -> tuple[int, int]:
    """Process a batch of shipments and return match statistics."""
    matched = 0
    unmatched = 0

    for doc in batch_docs:
        shipper_name = doc.get(SHIPMENTS_CFG["name"], "")
        shipper_country = doc.get(SHIPMENTS_CFG["jurisdiction"], "")
        #shipper_tokenized_name = doc.get(SHIPMENTS_CFG["tokenized_name"])

        best_match = matcher.find_best_match(shipper_name, shipper_country)

        #Update the document with the match result
        update = {
            "$set": {
                "mesur_entity": best_match.__dict__ if best_match else None,
                "last_matched": datetime.utcnow(),
            }
        }
        # update = {
        #     "$set": {
        #         "openc_entity": best_match.__dict__ if best_match else None,
        #         "last_matched": datetime.utcnow(),
        #     }
        # }

        shipments_collection.update_one({"_id": doc["_id"]}, update)

        if best_match:
            matched += 1
        else:
            unmatched += 1

    return matched, unmatched


def main():
    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    # Get collections
    shipments = db[SHIPMENTS_CFG["collection"]]
    entities = db[ENTITY_CFG["collection"]]

    # Create matcher instance
    matcher = EntityMatcher(entities)

    # Initialize counters
    total_processed = 0
    total_matched = 0
    total_unmatched = 0

    # Query for unprocessed documents
    query = {"mesur_entity": {"$exists": False}}
    # query = {"openc_entity": {"$exists": False}}
    total_docs = shipments.count_documents(query)

    logging.info("Starting processing of %s documents",total_docs)

    # Process in batches with progress bar
    with tqdm(total=total_docs, desc="Processing shipments") as pbar:
        while True:
            # Get next batch
            batch = list(shipments.find(query).limit(MATCHING_CFG["batch_size"]))

            if not batch:
                break

            # Process batch
            matched, unmatched = process_shipments_batch(matcher, shipments, batch)

            # Update counters
            total_matched += matched
            total_unmatched += unmatched
            total_processed += len(batch)

            # Update progress bar
            pbar.update(len(batch))

            # Log batch results
            logging.info(
                f"Batch completed - Matched: {matched}, "
                f"Unmatched: {unmatched}, "
                f"Total processed: {total_processed}/{total_docs}"
            )

    # Log final results
    logging.info("Processing completed!")
    logging.info(f"Total documents processed: {total_processed}")
    logging.info(f"Total matches found: {total_matched}")
    logging.info(f"Total unmatched: {total_unmatched}")
    logging.info(f"Match rate: {(total_matched/total_processed)*100:.2f}%")


if __name__ == "__main__":
    main()
