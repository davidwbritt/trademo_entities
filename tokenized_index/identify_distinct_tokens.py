from pymongo import MongoClient
from pymongo import ReadPreference

# Initialize MongoDB client and select database and collection
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"

client = MongoClient(MONGO_URI, socketTimeoutMS=8 * 60 * 60 * 1000)  # Set an 8-hour timeout
db = client[DATABASE]  
pipeline = [
    {
        "$group": {
            "_id": "$token"  # Group by 'token' field to get unique tokens
        }
    },
    {
        "$out": "distinct_tokens"  # Output to new collection 'distinct_tokens'
    }
]

# Run the pipeline with a .hint for the 'token_1' index
db.entity_token_index_filtered.with_options(read_preference=ReadPreference.PRIMARY).aggregate(
    pipeline,
    hint="token_1"
)
print("Distinct tokens have been successfully output to 'distinct_tokens'.")
