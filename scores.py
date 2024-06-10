import json

result_path = '/Users/kaushik/phd/cq-old/results/results.json'
response_path = '/Users/kaushik/phd/cq-old/results/answers.json'
query_path = '/Users/kaushik/phd/cq-old/queries/queries.json'

queries = json.load(open(query_path, 'r'))
answers = json.load(open(response_path, 'r'))

results = []

for answer in answers:
    nlq_i = 'English query: '
    nlq_e = '[/INST]'
    nlq = answer[answer.index(nlq_i) + len(nlq_i): answer.index(nlq_e)] 
    sql_i = 'SQL query:  '
    sql_e = ';'
    sqlq = answer[answer.index(sql_i) + len(sql_i):]
    if sql_e in sqlq:
        sqlq = sqlq[:sqlq.index(sql_e)]
    gsqlq = ''
    for query in queries:
        if nlq == query['naturalLanguageQuery']:
            gsqlq = query['goldSqlQuery']
    result = {
        'naturalLanguageQuery': nlq,
        'goldSqlQuery': gsqlq,
        'predictedQuery': sqlq
    }
    results.append(result)

json.dump(results, open(result_path, 'w'), indent=2)
