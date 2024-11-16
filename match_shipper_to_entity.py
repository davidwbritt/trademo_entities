import math
from collections import Counter, defaultdict

from bson import ObjectId
from pymongo import MongoClient

# MongoDB setup
MONGO_URI = "mongodb://172.17.0.4:27017"
DATABASE = "tradeverifyd"

client = MongoClient(MONGO_URI)
db = client[DATABASE]
shipments = db["trademo_entities"]
entities = db["opencorporates_entities"]
index = db["entity_token_index_final"]


# Calculate IDF for a given token
def calculate_idf(token, total_docs):
    token = token.upper()
    token_doc = index.find_one({"token": token})
    if token_doc:
        document_frequency = len(token_doc["entity_ids"])
        if total_docs > document_frequency:
            return math.log(total_docs / (1 + document_frequency))
        else:
            return 0.01  # Small value for very frequent tokens
    return 0


def calculate_jaccard_similarity(tokens1, tokens2):
    # Convert token lists to sets
    set1, set2 = set(tokens1), set(tokens2)
    # Calculate intersection and union
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    # Return Jaccard similarity score
    return intersection / union if union != 0 else 0


def find_best_matches(shipment, total_docs, top_n=5):
    tokenized_name = [token.upper() for token in shipment.get("tokenized_name", [])]
    shipment_normalized_name = shipment.get("normalized_name", "").upper()
    candidate_scores = defaultdict(float)
    candidate_token_count = Counter()

    for token in tokenized_name:
        idf = calculate_idf(token, total_docs)
        token_doc = index.find_one({"token": token})

        if token_doc:
            for entity_id in token_doc["entity_ids"]:
                if isinstance(entity_id, str):
                    entity_id = ObjectId(entity_id)
                candidate_scores[entity_id] += idf
                candidate_token_count[entity_id] += 1
        else:
            # Directly check common tokens in entities if not in the index
            for entity in entities.find({"tokenized_name": tokenized_name}):
                entity_id = entity["_id"]
                if token in entity["tokenized_name"]:
                    candidate_scores[entity_id] += 0.5  # Boost for common token match
                    candidate_token_count[entity_id] += 1

    # Additional Jaccard similarity check
    for entity_id in candidate_scores.keys():
        entity = entities.find_one({"_id": entity_id})
        if entity:
            entity_tokens = entity.get("tokenized_name", [])
            jaccard_score = calculate_jaccard_similarity(tokenized_name, entity_tokens)
            candidate_scores[
                entity_id
            ] += jaccard_score  # Add Jaccard score to total score

            # Additional boost for full normalized name match
            entity_normalized_name = entity.get("normalized_name", "").upper()
            if shipment_normalized_name == entity_normalized_name:
                candidate_scores[entity_id] += 5  # Additional boost for full name match

    # Rank entities by combined score and token overlap
    sorted_candidates = sorted(
        candidate_scores.items(),
        key=lambda item: (item[1], candidate_token_count[item[0]]),
        reverse=True,
    )[:top_n]

    # Fetch and display matched entities
    matches = []
    for entity_id, score in sorted_candidates:
        entity = entities.find_one({"_id": entity_id})
        if entity:
            entity_name = entity.get("name", "Unknown")
            jurisdiction = entity.get("jurisdiction", "Unknown")
            full_entity_name = f"{entity_name}, {jurisdiction}"
        else:
            full_entity_name = "Unknown"

        matches.append(
            {
                "entity": full_entity_name,
                "_id": entity_id,
                "score": score,
                "token_overlap": candidate_token_count[entity_id],
            }
        )
    return matches


def main():
    total_docs = entities.estimated_document_count()

    for shipment in shipments.find({}).limit(200):
        # Format shipment name and country
        shipment_name = shipment.get("trademo_name", "Unknown")
        shipment_country = shipment.get("country", "Unknown")

        # Retrieve best matches
        matches = find_best_matches(shipment, total_docs)

        # Print formatted output
        print(
            f"Best matches for shipment '{shipment['_id']}' ({shipment_name}, {shipment_country}):"
        )
        for match in matches:
            print(
                f"  - Entity: {match['entity']} | Score: {match['score']} | Token Overlap: {match['token_overlap']}"
            )
        print("-" * 50)


# Run the main function
if __name__ == "__main__":
    main()
