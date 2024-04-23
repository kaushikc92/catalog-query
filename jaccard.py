import pandas as pd
from py_stringmatching import QgramTokenizer, Jaccard

result_path = '/Users/kaushik/phd/cq-old/results/results.json'
df = pd.read_json(result_path)

def jaccard_score(x, y):
    qg3 = QgramTokenizer(qval=3)
    jac = Jaccard()
    return jac.get_raw_score(qg3.tokenize(x), qg3.tokenize(y))

df['jaccardScore'] = df.apply(lambda x: jaccard_score(x['goldSqlQuer'], x['predictedQuery'))
