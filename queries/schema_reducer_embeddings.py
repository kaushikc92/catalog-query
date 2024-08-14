from sentence_transformers import SentenceTransformer
from camelcase_tokenizer import CamelCaseTokenizer
import json
import numpy as np
import pdb

K = 5
SCHEMA_PATH = '../schema/sql/schema.json'
QUERY_PATH = '../queries/queries.json'
MODEL_PATH = 'BAAI/bge-large-en-v1.5'

schema = json.load(open(SCHEMA_PATH))
queries = json.load(open(QUERY_PATH))
schema_index = {}
documents = []
tokenizer = CamelCaseTokenizer()
for i, table in enumerate(schema):
    tokens = tokenizer.tokenize(table)
    doc = ' '.join(tokens) + ': '
    for j, column in enumerate(schema[table]):
        tokens = tokenizer.tokenize(column)
        doc += " ".join(tokens)
        if j == len(schema[table]) - 1:
            doc += '; '
        else:
            doc += ', '
    documents.append(doc)
    schema_index[i] = table

raw_queries = list(map(lambda x: x['naturalLanguageQuery'], queries))

model = SentenceTransformer(MODEL_PATH)
query_embeddings = model.encode(raw_queries, normalize_embeddings=True)
doc_embeddings = model.encode(documents, normalize_embeddings=True)
similarity = query_embeddings @ doc_embeddings.T
ranking = np.argpartition(similarity, -K)[:, -K:]

for i in range(len(queries)):
    queries[i]['tables_from_embeddings'] = [schema_index[j] for j in ranking[i]]
json.dump(queries, open(QUERY_PATH, 'w'), indent=2)
