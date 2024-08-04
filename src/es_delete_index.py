from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
INDEX = os.getenv('ELASTICSEARCH_INDEX') 
ES_HOST = f"http://localhost:{os.getenv('ELASTICSEARCH_HTTP_PORT', 9200)}"
client = Elasticsearch([ES_HOST])
client.indices.delete(index=INDEX)
