from elasticsearch import Elasticsearch
from settings import SETTINGS
import json
import pdb
from camelcase_tokenizer import CamelCaseTokenizer


K = 5
SCHEMA_PATH = '../schema/sql/schema.json'
QUERY_PATH = '../queries/queries.json'
INDEX = 'column_catalog'

queries = json.load(open(QUERY_PATH))

# client = Elasticsearch(
#     "https://localhost:9200", 
#     ca_certs = "/Users/kaushik/dev/elasticsearch-8.13.2/config/certs/http_ca.crt", 
#     basic_auth=("elastic", "BdDJGF3eGab70KOZThpt")
# )
client = Elasticsearch(
    "https://localhost:9200", 
    ca_certs = "../build-catalog/elasticsearch-8.15.0/config/certs/http_ca.crt", 
    basic_auth=("elastic","dPeHUSB9XN7hQAxC=jRl")
)

def build_index():
    client.indices.create(index=INDEX, body=SETTINGS)
    schema = json.load(open(SCHEMA_PATH))
    tokenizer = CamelCaseTokenizer()
    for i, table in enumerate(schema):
        tokens = tokenizer.tokenize(table)
        concatenated_string = ' '.join(tokens)
        body = {"table_name" : "table " + concatenated_string}

        for j, column in enumerate(schema[table]):
            tokens = tokenizer.tokenize(column)
            concatenated_string = " ".join(tokens)
            body["column_name_" + str(j)] = concatenated_string
    
        client.index(
            index = "column_catalog",
            id = table,
            document=body
        )

def search_index(query):
    client.indices.refresh(index=INDEX)
    result = client.search(index="column_catalog", body = {
        "size": K,
        "query": {
           "multi_match": {
                "query": query,
                "fields": []
            }
        }
    })
    res = []
    for item in result['hits']['hits']:
        res.append(item['_id'])
    return res

def delete_index():
    client.indices.delete(index=INDEX)

build_index()
    
for query in queries:
    query['tables_from_tfidf'] = search_index(query['naturalLanguageQuery'])

json.dump(queries, open(QUERY_PATH, 'w'), indent=2)

delete_index()
