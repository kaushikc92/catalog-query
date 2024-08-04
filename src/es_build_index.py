from elasticsearch import Elasticsearch
from utils.camelcase_tokenizer import CamelCaseTokenizer
import json, os
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

SCHEMA_PATH = os.getenv('SQL_SCHEMA_PATH')
K = 5
INDEX = os.getenv('ELASTICSEARCH_INDEX') 
ES_HOST = f"http://localhost:{os.getenv('ELASTICSEARCH_HTTP_PORT', 9200)}"

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

tokenizer = CamelCaseTokenizer()
schema = json.load(open(SCHEMA_PATH))
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
