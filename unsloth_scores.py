import json


result_path = './results.json'
response_path = './answers.json'
query_path = './queries/queries.json'

queries = json.load(open(query_path, 'r'))
answers = json.load(open(response_path, 'r'))

results = []

for answer in answers:
    nlq_i = 'English query: '
    nlq_e = '\n'
    nlq = answer[answer.index(nlq_i) + len(nlq_i): answer.index(nlq_e, answer.index(nlq_i))] 
    sql_i = 'select'
    sql_e = ';'
    try:
        start_index = answer.index('### Response')
        if sql_i in answer[start_index + len('### Response'):].lower():
            sqlq_start_index = answer.lower().index(sql_i, start_index + len('### Response'))
        else:
            sqlq_start_index = start_index + len('### Response')
        sqlq = answer[sqlq_start_index:]
    except ValueError as e:
        print(f"sql query was not found.")
        print(answer[start_index:])
        print()
        sqlq = ""

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
