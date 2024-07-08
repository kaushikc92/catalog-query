from py_stringmatching import QgramTokenizer, Jaccard
import json
from post_processor import SQLValidator

#RESULTS_PATH = '/Users/kaushik/phd/cq-old/results/results.json'
THRESHOLD_SCORE = 0.75
results = json.load(open(RESULTS_PATH))

def jaccard_score(x, y):
    qg3 = QgramTokenizer(qval=3)
    jac = Jaccard()
    return jac.get_raw_score(qg3.tokenize(x), qg3.tokenize(y))

n = len(results)
p = 0
r = 0
validator = SQLValidator(schema_match=True)
for res in results:
    if validator.validate_query(res['predictedQuery']):
        r += 1
        if jaccard_score(res['goldSqlQuery'], res['predictedQuery']) > THRESHOLD_SCORE:
            p += 1
print(f'Total Samples: {n}')
print(f'Precision: {p}/{r} = {p/r}')
print(f'Recall: {p}/{n} = {p/n}')

