from py_stringmatching import QgramTokenizer, Jaccard
import json
import sqlvalidator

#RESULTS_PATH = '/Users/kaushik/phd/cq-old/results/results.json'
THRESHOLD_SCORE = 0.75
results = json.load(open(RESULTS_PATH))

def jaccard_score(x, y):
    qg3 = QgramTokenizer(qval=3)
    jac = Jaccard()
    return jac.get_raw_score(qg3.tokenize(x), qg3.tokenize(y))

n = len(results)
print(f'Total Samples: {n}')
p = 0
r = 0
for res in results:
    try:
        sql_query = sqlvalidator.parse(res['predictedQuery'])
        if sql_query.is_valid():
            r += 1
            if jaccard_score(res['goldSqlQuery'], res['predictedQuery']) > THRESHOLD_SCORE:
                p += 1
    except:
        pass
print(f'Precision: {p}')
print(f'Recall: {r}')
