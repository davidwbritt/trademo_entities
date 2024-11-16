1. split_csv.py
2. opencorporates_raw.py
3. opencorporates_entities.py
4. tokenize_opencorporates_names.py (create index on tokenized_name before running; check name field matches collection value)
5. tokenize_trademo_names.py (update to tokenize trademo_entities name by the same rules)
6. create_inverted_index.py (will index tokenized name entries from opencorporates_entities; needs consolidation afterward)
7. prune_inverted_index.py -- this will create entity_token_index_filtered which will have removed the token documents with chunk >= 1
8. identify_distinct_tokens.py
8. merge_tokens_inverted_index.py -- combines documents by token