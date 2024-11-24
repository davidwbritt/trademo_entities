import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def run_pipeline(mongo_uri, database_name, collection_name="opencorporates_raw"):
    try:
        # Set up MongoDB client with a high timeout
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8 * 60 * 60 * 1000) 
        db = client[database_name]
        collection = db[collection_name]

        # Define the aggregation pipeline
        pipeline = [
            {
                "$match": {
                    "inactive": { "$ne": True }
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "company_number": 1,
                    "name": 1,
                    "normalised_name": 1,
                    "jurisdiction_code": 1,
                    "company_type": 1,
                    "home_jurisdiction_code": 1,
                    "home_jurisdiction_text": 1,
                    "current_status": 1,
                    "registered_address": 1,
                    "current_alternative_legal_name": 1,
                    "previous_names": 1,
                    "inactive": 1
                }
            },
            {
                "$out": "opencorporates_entities"
            }
        ]

        # Run the aggregation with a high timeout and allowDiskUse set to True
        result = collection.aggregate(pipeline, allowDiskUse=True, maxTimeMS=8 * 60 * 60 * 1000)  # 8 hours in milliseconds
        if result:
            logging.info("Pipeline executed successfully.")

    except PyMongoError as e:
        logging.error("An error occurred: %s ",e)
    finally:
        client.close()

def main():
    # MongoDB URI and database name
    mongo_uri = "mongodb://172.17.0.4:27017"
    database_name = "tradeverifyd"  # Update with your database name

    # Run the pipeline function
    run_pipeline(mongo_uri, database_name)

if __name__ == "__main__":
    main()
