from elasticsearch import Elasticsearch
import json, os

K = 5
INDEX = os.getenv('ELASTICSEARCH_INDEX') 
ES_HOST = f"http://localhost:{os.getenv('ELASTICSEARCH_HTTP_PORT', 9200)}"
SQL_SCHEMA_PATH = os.getenv('SQL_SCHEMA_PATH')
client = Elasticsearch([ES_HOST])

SETTINGS = {
	"settings": {
    	"analysis": {
            "filter": {
                "my_ngram_filter": {
                    "type": "ngram",
                    "min_gram": 3,
                    "max_gram": 5
                },
            },
            "analyzer": {
                "my_custom_analyzer": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "my_ngram_filter"]
                }
            }
        },
        "index": {
            "max_ngram_diff": 5 
        }
    },
    "mappings": {
        "dynamic_templates": [
            {
                "text_fields_use_customized_tokenizer": {
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "text",
                        "analyzer": "my_custom_analyzer"
                    }
                }
            }
        ]
    }
}

def build_index(tokenizer):
    schema = json.load(open(SQL_SCHEMA_PATH))
    client.indices.create(index=INDEX, body=SETTINGS)
    for i, table in enumerate(schema):
        tokens = tokenizer.tokenize(table)
        concatenated_string = ' '.join(tokens)
        body = {"table_name" : "table " + concatenated_string}

        for j, column in enumerate(schema[table]):
            tokens = tokenizer.tokenize(column)
            concatenated_string = " ".join(tokens)
            body["column_name_" + str(j)] = concatenated_string

        client.index(
            index = INDEX,
            id = table,
            document=body
        )

def search_index(nl_query):
    client.indices.refresh(index=INDEX)
    result = client.search(index=INDEX, body = {
        "size": K,
        "query": {
           "multi_match": {
                "query": nl_query,
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

def add_schema_from_tfidf(example_attributes, tokenizer):
    for eg_attrib in example_attributes:
        nl_query = eg_attrib['naturalLanguageQuery']
        eg_attrib['tablesFromTfIdf'] = search_index(nl_query)
    return example_attributes
