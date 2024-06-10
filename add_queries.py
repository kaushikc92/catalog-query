import json, sys, re, os
QUERY_JSON_PATH = 'queries/queries.json'
QUERY_PATH = 'queries'
SCHEMA_PATH = 'schema/sql/schema.json'

schema = json.load(open(SCHEMA_PATH, 'r'))
queries = []

for filename in os.listdir(QUERY_PATH):
    if filename[-4:] != '.txt':
        continue
    queryfile = open(QUERY_PATH + '/' + filename, 'r')
    lines = queryfile.readlines()
    nlq = ''
    sqlq = ''

    for i, line in enumerate(lines):
        if i % 3 == 0:
            nlq = line[:-1]
        elif i % 3 == 1:
            sqlq = line[:-1]
        else:
            tables = []
            tokens = sqlq.split(' ')
            for token in tokens:
                if token in schema:
                    tables.append(token)
            query = {
                'naturalLanguageQuery': nlq,
                'goldSqlQuery': sqlq,
                'isAggregate': len(re.findall(' group by ', sqlq)) != 0,
                'isConditional': len(re.findall(' where ', sqlq)) != 0 or len(re.findall(' having ', sqlq)) != 0,
                'tables': tables
            }
            queries.append(query)
print(f'Queries added = {len(queries)}')
json.dump(queries, open(QUERY_JSON_PATH, 'w'), indent=2)
