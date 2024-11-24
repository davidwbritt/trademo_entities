# config.py
from typing import TypedDict

class CollectionConfig(TypedDict):
    collection: str
    jurisdiction: str
    name: str
    normalized_name: str
    tokenized_name: str
    entity_uid: str | None

class MatchingConfig(TypedDict):
    min_token_length: int
    stopwords: list[str]
    name_similarity_weight: float
    jurisdiction_weight: float
    exact_jurisdiction_score: float
    neighboring_jurisdiction_score: float
    non_matching_jurisdiction_score: float
    min_score_threshold: float
    max_search_results: int
    batch_size: int

MONGODB_URI = "mongodb://172.17.0.4:27017"
DB_NAME = "tradeverifyd"

SHIPMENTS_CFG: CollectionConfig = {
    "collection": "trademo_sourced_entities",
    "jurisdiction": "jurisdiction",
    "name": "name",
    "normalized_name": "name",
    "tokenized_name": "tokenized_name",
    "entity_uid": None
}

ENTITY_CFG: CollectionConfig = {
    "collection": "mesur.io_entities_notrademo",
    "jurisdiction": "jurisdiction",
    "name": "name",
    "normalized_name": "name",
    "entity_uid": "_id",
    "tokenized_name": "tokenized_name",
}

# ENTITY_CFG: CollectionConfig = {
#     "collection": "opencorporates_entities",
#     "jurisdiction": "jurisdiction_code",
#     "name": "name",
#     "normalized_name": "normalised_name",
#     "entity_uid": "company_number",
#     "tokenized_name": "tokenized_name",
# }


#these stopwords will not be part of the query to identify entity-match-candidates
MATCHING_CFG: MatchingConfig = {
    "min_token_length": 2,
    "stopwords": [
        "VARIABLE",
        "SOCIEDAD",
        "CAPITAL",
        "ANONIMA",
        "LIMITED",
        "LIABILITY",
        "COMPANY",
        "GESELLSCHAFT",
        "BESCHRÄNKTER",
        "HAFTUNG",
        "INTERNATIONAL",
        "INDIA",
        "CHINA",
        "ENTERPRISES",
        "EXPORTS",
        "IMPORTS",
        "IMPORT",
        "EXPORT",
        "TRADING",
        "CÔNG",
        "CONG",
        "VIET",
        "NAM",
        "TNHH",
        "PRIVATE",
        "ENGINEERS",
        "HANDICRAFTS",
        "FABRICS"
    ],
    "name_similarity_weight": 0.7,
    "jurisdiction_weight": 0.3,
    "exact_jurisdiction_score": 1.0,
    "neighboring_jurisdiction_score": 0.5,
    "non_matching_jurisdiction_score": 0.0,
    "min_score_threshold": 0.55,
    "max_search_results": 20,
    "batch_size": 5000,
}
