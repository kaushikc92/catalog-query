from elasticsearch import Elasticsearch
INDEX = 'column_catalog'
ES_HOST='http://localhost:9200'
client = Elasticsearch([ES_HOST])
client.indices.delete(index=INDEX)
