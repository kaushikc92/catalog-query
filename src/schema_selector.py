from sentence_transformers import SentenceTransformer
from camelcase_tokenizer import CamelCaseTokenizer
import json
import numpy as np
import pdb

K = 5
SQL_SCHEMA_PATH = '../config/schema/sql_schema.json'
EXAMPLE_ATTRIBUTES_PATH = '../data/example_attributes.json'
MODEL_PATH = 'BAAI/bge-large-en-v1.5'

schema = json.load(open(SQL_SCHEMA_PATH, 'r'))
examples = json.load(open(EXAMPLE_ATTRIBUTES_PATH))

def add_schema_from_embeddings():
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

    raw_queries = list(map(lambda x: x['naturalLanguageQuery'], examples))

    model = SentenceTransformer(MODEL_PATH)
    query_embeddings = model.encode(raw_queries, normalize_embeddings=True)
    doc_embeddings = model.encode(documents, normalize_embeddings=True)
    similarity = query_embeddings @ doc_embeddings.T
    ranking = np.argpartition(similarity, -K)[:, -K:]

    for i in range(len(examples)):
        examples[i]['tablesFromEmbeddings'] = [schema_index[j] for j in ranking[i]]
    json.dump(queries, open(EXAMPLE_ATTRIBUTES_PATH, 'w'), indent=2)

if __name__ == "__main__":
    add_schema_from_embeddings()
