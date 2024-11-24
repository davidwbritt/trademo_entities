import logging
import re
import sys
import time
from typing import Optional

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError


def tokenize_name(name: str) -> list[str]:
    """
    Generate tokens for company name matching, including stopwords.
    Returns only individual word tokens in uppercase, with special characters removed.
    """
    if not name:
        return []

    name = name.upper()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"[^\w\s]", "", name)
    name = name.strip()

    return list(set(name.split()))


def tokenize_names_batch(
    mongo_uri: str,
    database_name: str,
    collection_name: str,
    source_name_field: str,
    batch_size: int = 1000,
    max_bulk_ops: int = 1000,
    logging_enabled: bool = True,
) -> tuple[bool, Optional[str]]:
    """
    Process documents in batches to add tokenized names where missing.

    Args:
        mongo_uri: MongoDB connection string
        database_name: Name of the database
        collection_name: Name of the collection
        source_name_field: Field containing the normalized name to tokenize
        batch_size: Number of documents to process in each query
        max_bulk_ops: Maximum number of operations in a bulk write
        logging_enabled: Whether to enable logging
    """
    try:
        if logging_enabled:
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)

        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        start_time = time.time()

        if logging_enabled:
            logger.info("Getting count of documents to process")

        total_to_process = collection.count_documents(
            {"tokenized_name": {"$exists": False}}
        )

        if logging_enabled:
            logger.info(f"Found {total_to_process:,} documents to process")

        if total_to_process == 0:
            if logging_enabled:
                logger.info("No documents need processing")
            return True, None

        processed_count = 0
        batch_count = 0

        while processed_count < total_to_process:
            cursor = collection.find(
                {"tokenized_name": {"$exists": False}}, {"_id": 1, source_name_field: 1}
            ).limit(batch_size)

            bulk_operations: list[UpdateOne] = []

            for doc in cursor:
                name = doc.get(source_name_field, "")
                tokenized_name = tokenize_name(name)

                update_op = UpdateOne(
                    {"_id": doc["_id"]}, {"$set": {"tokenized_name": tokenized_name}}
                )
                bulk_operations.append(update_op)

                if len(bulk_operations) >= max_bulk_ops:
                    result = collection.bulk_write(bulk_operations)
                    processed_count += result.modified_count
                    bulk_operations = []

                    if logging_enabled:
                        logger.info(
                            f"Processed {processed_count:,}/{total_to_process:,} documents"
                        )

            if bulk_operations:
                result = collection.bulk_write(bulk_operations)
                processed_count += result.modified_count

            batch_count += 1

            if logging_enabled:
                logger.info(f"Completed batch {batch_count}")
                logger.info(
                    f"Total processed: {processed_count:,}/{total_to_process:,} documents"
                )

        collection.create_index("tokenized_name", unique=False)

        if logging_enabled:
            elapsed_time = time.time() - start_time
            logger.info(f"Processing completed in {elapsed_time:.2f} seconds")
            logger.info(f"Total documents processed: {processed_count:,}")

        return True, None

    except PyMongoError as e:
        error_msg = f"MongoDB error: {str(e)}"
        if logging_enabled:
            logger.error(error_msg)
        return False, error_msg

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        if logging_enabled:
            logger.error(error_msg)
        return False, error_msg

    finally:
        if "client" in locals():
            client.close()


def main():
    collections = [
        # {
        #     "name": "trademo_entities",
        #     "source_field": "normalized_name"
        # }
        # ,
        # {
        #     "name": "opencorporates_entities",
        #     "source_field": "normalised_name"
        # },
        {"name": "trademo_sourced_entities", "source_field": "name"}
    ]
    # collections = [
    #     {
    #         "name": "mesur.io_entities",
    #         "source_field": "name"
    #     }
    # ]
    mongo_uri = "mongodb://172.17.0.2:27017"
    database_name = "tradeverifyd"
    batch_size = 10000
    max_bulk_ops = 10000
    logging_enabled = True
    max_retries = 5
    retry_delay = 60

    for collection in collections:
        for attempt in range(1, max_retries + 1):
            success, error = tokenize_names_batch(
                mongo_uri=mongo_uri,
                database_name=database_name,
                collection_name=collection["name"],
                source_name_field=collection["source_field"],
                batch_size=batch_size,
                max_bulk_ops=max_bulk_ops,
                logging_enabled=logging_enabled,
            )

            if success:
                print(f"Processing completed successfully for {collection['name']}")
                break
            else:
                print(
                    f"Attempt {attempt} failed for {collection['name']} with error: {error}",
                    file=sys.stderr,
                )

                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(
                        f"Max retries reached for {collection['name']}. Exiting with failure.",
                        file=sys.stderr,
                    )
                    sys.exit(1)


if __name__ == "__main__":
    main()
