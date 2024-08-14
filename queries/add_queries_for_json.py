import json,re
QUERY_JSON_PATH = 'queries.json'
SCHEMA_PATH = '../schema/sql/schema.json'

schema = json.load(open(SCHEMA_PATH, 'r'))
queries = []

data = json.load(open(QUERY_JSON_PATH, 'r'))

for query in data:
    print(query)
    sqlq = query['goldSqlQuery']
    tables = []
    tokens = sqlq.split(' ')
    for token in tokens:
        if token in schema:
            tables.append(token)
    new_query = {
        'eid': query['eid'],
        'naturalLanguageQuery': query['naturalLanguageQuery'],
        'goldSqlQuery': sqlq,
        'isAggregate': len(re.findall(' group by ', sqlq)) != 0,
        'isConditional': len(re.findall(' where ', sqlq)) != 0 or len(re.findall(' having ', sqlq)) != 0,
        'tables': tables
    }
    queries.append(new_query)
print(f'Queries added = {len(queries)}')
json.dump(queries, open(QUERY_JSON_PATH, 'w'), indent=2)
