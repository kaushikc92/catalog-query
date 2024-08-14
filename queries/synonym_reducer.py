import json
import sentence_transformers
from nltk.stem import *
import re
 
INSTANCE_SYNS_FILE = '../schema/instance_syns.json'
SCHEMA_SYNS_FILE = '../schema/schema_syns.json'
QUERY_FILE = 'queries.json'
MODEL = 'intfloat/e5-base-v2'
TOPK = 5

def is_in_query_as_word(value, english_query):
    # \b denotes a word boundary in regex
    pattern = r'\b' + re.escape(value) + r'\b'
    return re.search(pattern, english_query, re.IGNORECASE) is not None

def find_exact_match_synonym(english_query, synonyms_dict):
    synonym_matches = []
    stemmer = PorterStemmer()
    if isinstance(english_query, str):
        english_query = ' '.join([stemmer.stem(word, to_lowercase=True) for word in english_query.split()])
        for key, values in synonyms_dict.items():
            # stem_key = stemmer.stem(key, to_lowercase=True)
            for value in values:
                # if key in prompt:
                #     synonym_matches.append((key, value))
                stem_value = stemmer.stem(value, to_lowercase=True)
                #print(key, value, english_query)
                if is_in_query_as_word(stem_value, english_query):
                    synonym_matches.append((key, value))
        return synonym_matches
    else:
        return synonym_matches
    
def tokenize_to_phrase(sentence):
    phrases = []
    words = sentence.split()
    for i in range(len(words)):
        # One-word phrase
        phrases.append(words[i])
        # Two-word phrase
        if i < len(words) - 1:
            phrases.append(words[i] + ' ' + words[i+1])
        # Three-word phrase
        if i < len(words) - 2:
            phrases.append(words[i] + ' ' + words[i+1] + ' ' + words[i+2])
    return phrases

def find_similar_synonyms(model, english_queries, synonyms_dict_with_embedding, topk):
    if not isinstance(english_queries, list):
        return []
    
    # Tokenize all queries into phrases with one word, two words, and three words
    all_phrases = []
    query_phrase_mapping = []

    for english_query in english_queries:
        english_query_phrases = tokenize_to_phrase(english_query)
        all_phrases.extend(['query: ' + phrase for phrase in english_query_phrases])
        query_phrase_mapping.extend([english_query] * len(english_query_phrases))

    # Batch encode all phrases
    embedding_phrases = model.encode(all_phrases, normalize_embeddings=True)
    
    # Initialize a dictionary to hold synonym matches for each query
    all_synonym_matches = {query: [] for query in english_queries}

    for key, values in synonyms_dict_with_embedding.items():
        for value, embedding_value in values.items():
            # Compute cosine similarity between the batched query embeddings and each value embedding
            similarities = sentence_transformers.util.cos_sim(embedding_phrases, embedding_value)
            
            # Iterate over each query's similarities and collect the top matches
            for query, similarity in zip(query_phrase_mapping, similarities):
                all_synonym_matches[query].append((key, value, similarity.item()))

    # For each query, sort the synonym matches by similarity and retain the topk results
    final_matches = {}
    for query, matches in all_synonym_matches.items():
        top_matches = sorted(matches, key=lambda x: x[2], reverse=True)[:topk]
        final_matches[query] = [(match[0], match[1]) for match in top_matches]
    
    return final_matches


f = open(INSTANCE_SYNS_FILE)
instance_syns = json.load(f)
f.close()

f = open(SCHEMA_SYNS_FILE)
schema_syns = json.load(f)
f.close()

compact_schema_syns = {}
for key, values in schema_syns.items():
    new_key = key.split('.')[-1]

    # Check if the new_key already exists and handle conflicts by appending values
    if new_key in compact_schema_syns:
        for value in values:
            if value not in compact_schema_syns[new_key]:
                compact_schema_syns[new_key].append(value)
    else:
        compact_schema_syns[new_key] = values

synonym_file = {**instance_syns, **compact_schema_syns}

# prepare the embedding for the catalog
model = sentence_transformers.SentenceTransformer(MODEL)

# Prepare to encode all values in a single batch
all_values = []
key_value_mapping = []

for key, values in synonym_file.items():
    for value in values:
        all_values.append(f'query: {value}')
        key_value_mapping.append((key, value))

# Encode all values in a single batch
all_embeddings = model.encode(all_values, normalize_embeddings=True)

# Distribute embeddings back to their corresponding keys
synonym_file_with_embedding = {}
for (key, value), embedding in zip(key_value_mapping, all_embeddings):
    if key in synonym_file_with_embedding:
        synonym_file_with_embedding[key][value] = embedding
    else:
        synonym_file_with_embedding[key] = {value: embedding}


data = json.load(open(QUERY_FILE, 'r'))
# for query in data:
#     query['synonyms_from_exact_match'] = find_exact_match_synonym(query['naturalLanguageQuery'], synonym_file)
    #query['synonyms_from_embeddings'] = find_similar_synonyms(model, query['naturalLanguageQuery'], synonym_file_with_embedding, TOPK)

nl_queries = [query["naturalLanguageQuery"] for query in data]
final_matches = find_similar_synonyms(model, nl_queries, synonym_file_with_embedding, TOPK)

# Add the final results back to each query
for query in data:
    nl_query = query["naturalLanguageQuery"]
    query['synonyms_from_exact_match'] = find_exact_match_synonym(query['naturalLanguageQuery'], synonym_file)
    query["synonyms_from_embeddings"] = final_matches.get(nl_query, [])

json.dump(data, open(QUERY_FILE, 'w'), indent=2)