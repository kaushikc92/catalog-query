import json, random
from datasets import load_dataset, Dataset
TRAIN_PATH = 'kaushikchan/catalog-sql-train-alpaca'
TEST_PATH = 'kaushikchan/catalog-sql-test-alpaca'
N_TRAIN = 500
N_TEST = 700
SCHEMA_PATH = 'schema/sql/schema.json'
QUERY_PATH = 'queries/queries.json'

schema = json.load(open(SCHEMA_PATH, 'r'))
instruction = 'Translate english queries to SQL using the given schema.'
query_dataset = load_dataset("json", data_files=QUERY_PATH, split='train')
query_dataset = query_dataset.train_test_split(train_size=N_TRAIN, test_size=N_TEST, seed=10086)
instructions = [instruction for _ in range(N_TRAIN)]
inputs, outputs = [], []
for query in query_dataset['train']:
    context = 'Schema: '
    tables = query['tables']
    n_tables = random.randint(5, 10)
    while len(tables) < n_tables:
        table = random.choice(list(schema.keys()))
        if table not in tables:
            tables.append(table)
    random.shuffle(tables)
    for table in tables:
        context += table + ': '
        for column in schema[table]:
            context += column + ' '
        context += '; '
    nlq = 'English query: ' + query['naturalLanguageQuery']
    inp = f"{context}\n{nlq}"
    inputs.append(inp)
    outputs.append(query['goldSqlQuery'] + ';')
train_set = Dataset.from_dict({
    'instruction': instructions,
    'input': inputs,
    'output': outputs
})
train_set.push_to_hub(TRAIN_PATH)

instructions = [instruction for _ in range(N_TEST)]
inputs = []
for query in query_dataset['test']:
    context = 'Schema: '
    s1 = set(query['tables_from_tfidf'])
    s2 = set(query['tables_from_embeddings'])
    tables = list(s1 | s2)
    random.shuffle(tables)
    for table in tables:
        context += table + ': '
        for column in schema[table]:
            context += column + ' '
        context += '; '
    nlq = 'English query: ' + query['naturalLanguageQuery']
    inp = f"{context}\n{nlq}"
    inputs.append(inp)
test_set = Dataset.from_dict({
    'instruction': instructions,
    'input': inputs
})
test_set.push_to_hub(TEST_PATH)
