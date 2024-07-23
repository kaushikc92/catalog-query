import json, re, csv
from elasticsearch import Elasticsearch

EXAMPLES_PATH = '../data/examples.csv'
EXAMPLE_ATTRIBUTES_PATH = '../data/example_attributes.csv'
SQL_SCHEMA_PATH = '../config/schema/sql_schema.json'
K = 5
INDEX = 'column_catalog'
ES_HOST='http://localhost:9200'

client = Elasticsearch([ES_HOST])
schema = json.load(open(SQL_SCHEMA_PATH, 'r'))

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

def generate_example_attributes():
    examples = []
    with open(EXAMPLES_PATH, mode='r') as f:
        csv_reader = csv.DictReader(f)
        for i, row in enumerate(csv_reader):
            nl_query = row['NL_query']
            gold_sql_query = row['Gold_SQL_query']
            tables = []
            tokens = gold_sql_query.split(' ')
            for token in tokens:
                if token in schema:
                    tables.append(token)
            example = {
                'eid': 'e' + str(i),
                'naturalLanguageQuery': nl_query,
                'goldSqlQuery': gold_sql_query,
                'isAggregate': len(re.findall(' group by ', gold_sql_query)) != 0,
                'isConditional': len(re.findall(' where ', gold_sql_query)) != 0 or len(re.findall(' having ', gold_sql_query)) != 0,
                'tables': tables,
                'tablesFromTfIdf': search_index(nl_query)
            }
            examples.append(example)
    print(f'Generated attributes for {len(examples)} examples')
    json.dump(examples, open(EXAMPLE_ATTRIBUTES_PATH, 'w'), indent=2)

if __name__ == "__main__":
    generate_example_attributes()
