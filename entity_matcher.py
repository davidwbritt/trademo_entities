import re
from typing import Set, Optional
from dataclasses import dataclass
from pymongo.collection import Collection
from config import MATCHING_CFG, ENTITY_CFG
from jurisdiction_neighborhood import (
    get_regional_jurisdictions_by_country,
    get_regional_jurisdictions,
    get_iso_code_by_country,
)


@dataclass
class MatchResult:
    source_collection: str
    name: str
    jurisdiction: str
    score: float
    company_number: str


class EntityMatcher:
    def __init__(self, entities_collection: Collection):
        self.entities = entities_collection

    @staticmethod
    def clean_name(name: str) -> str:
        if not isinstance(name, str):
            return ""
        return re.sub(r"[^\w\s]", " ", name).strip()

    @staticmethod
    def tokenize_name(name: str) -> Set[str]:
        if not isinstance(name, str):
            return set()
        cleaned = EntityMatcher.clean_name(name)
        return {word.upper() for word in cleaned.split()}

    @staticmethod
    def prepare_tokens_for_search(tokens: Set[str]) -> list[str]:
        if not tokens:
            return []

        tokens_list = [
            word
            for word in tokens
            if len(word) >= MATCHING_CFG["min_token_length"]
            and word not in MATCHING_CFG["stopwords"]
        ]

        return tokens_list

    def find_best_match(
        self, shipper_name: str, shipping_country: str
    ) -> Optional[MatchResult]:
        """Find the best matching entity above the threshold score."""

        name_without_country = " ".join(shipper_name.split()[:-1])
        # shipper_tokens = self.tokenize_name(shipper_name)
        shipper_tokens = self.tokenize_name(name_without_country)

        tokens = self.prepare_tokens_for_search(shipper_tokens)

        if not tokens:
            return None

        query = {"tokenized_name": {"$all": tokens}}
        best_match = None
        best_score = MATCHING_CFG["min_score_threshold"]

        # regional_jurisdictions = get_regional_jurisdictions_by_country(shipping_country)
        shipper_code = (
            shipping_country.upper()
            if len(shipping_country) == 2
            else get_iso_code_by_country(shipping_country)
        )
        regional_jurisdictions = get_regional_jurisdictions(shipper_code)

        for entity in (
            self.entities.find(query)
            .limit(MATCHING_CFG["max_search_results"])
            .hint("tokenized_name_1")
        ):
            entity_tokens = set(entity.get("tokenized_name", []))
            entity_jurisdiction = entity.get("jurisdiction", "")
            entity_main_jurisdiction = entity_jurisdiction.upper().split("_")[0]

            score = self._calculate_match_score(
                shipper_tokens,
                entity_tokens,
                shipper_code,
                entity_main_jurisdiction,
                regional_jurisdictions,
            )

            if score > best_score:
                best_score = score
                best_match = MatchResult(
                    source_collection=ENTITY_CFG["collection"],
                    name=entity.get("name", ""),
                    jurisdiction=entity_jurisdiction,
                    score=score,
                    company_number=str(entity.get("_id", "")),
                )

                # If we found a perfect match, return immediately
                if score == 1.0:
                    return best_match

        return best_match

    def _calculate_match_score(
        self,
        trademo_tokens: set[str],
        entity_tokens: set[str],
        shipper_code: str,
        entity_jurisdiction: str,
        regional_jurisdictions: list[str],
    ) -> float:
        if not trademo_tokens or not entity_tokens:
            return 0.0

        # Name similarity score
        intersection = len(trademo_tokens & entity_tokens)
        union = len(trademo_tokens | entity_tokens)
        name_score = intersection / union if union > 0 else 0.0

        # Jurisdiction score
        jurisdiction_score = MATCHING_CFG["non_matching_jurisdiction_score"]
        if entity_jurisdiction.upper() == shipper_code.upper():
            jurisdiction_score = MATCHING_CFG["exact_jurisdiction_score"]
        elif entity_jurisdiction.upper() in [j.upper() for j in regional_jurisdictions]:
            jurisdiction_score = MATCHING_CFG["neighboring_jurisdiction_score"]

        # Calculate final weighted score
        final_score = (
            name_score * MATCHING_CFG["name_similarity_weight"]
            + jurisdiction_score * MATCHING_CFG["jurisdiction_weight"]
        )

        return final_score
