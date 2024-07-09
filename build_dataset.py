import json, random
from datasets import load_dataset, Dataset
from tfidf import build_index, search_index, delete_index
train_path = 'kaushikchan/catalog-sql-train'
test_path = 'kaushikchan/catalog-sql-test'

schema_path = 'schema/sql/schema.json'
schema = json.load(open(schema_path, 'r'))
prompts = []
instruction = 'Translate english queries to SQL.'
query_path = 'queries/queries.json'
query_dataset = load_dataset("json", data_files=query_path, split='train')
query_dataset = query_dataset.train_test_split(train_size=500, test_size=700, seed=10086)

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
    sqlq = 'SQL query: ' + query['goldSqlQuery'] + ';'
    prompt = f"[INST] <<SYS>>{instruction}<</SYS>> {context} {nlq}[/INST] {sqlq}"
    prompts.append(prompt)
train_set = Dataset.from_dict({'text': prompts})
train_set.push_to_hub(train_path)

prompts = []
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
    sqlq = 'SQL query: '
    prompt = f"[INST] <<SYS>>{instruction}<</SYS>> {context} {nlq}[/INST] {sqlq}"
    prompts.append(prompt)
test_set = Dataset.from_dict({'text': prompts})
test_set.push_to_hub(test_path)
