import json, random
from datasets import load_dataset, Dataset
from tfidf import build_index, search_index, delete_index

schema_path = '/Users/kaushik/phd/cq-old/schema/schema.json'
schema = json.load(open(schema_path, 'r'))
prompts = []
instruction = 'Translate english queries to SQL.'
query_path = '/Users/kaushik/phd/cq-old/queries/queries.json'
query_dataset = load_dataset("json", data_files=query_path, split='train')
query_dataset = query_dataset.train_test_split(train_size=400, test_size=150)

for query in query_dataset['train']:
    context = 'Schema: '
    tables = query['tables']
    while len(tables) < 5:
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
    sqlq = 'SQL query: ' + query['goldSqlQuery']
    prompt = f"[INST] <<SYS>>{instruction}<</SYS>> {context} {nlq}[/INST] {sqlq}"
    prompts.append(prompt)
train_set = Dataset.from_dict({'text': prompts})
train_set.push_to_hub('kaushikchan/catalog-sql-small-train')

prompts = []
for query in query_dataset['test']:
    context = 'Schema: '
    tables = search_index(query['naturalLanguageQuery'])
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
test_set.push_to_hub('kaushikchan/catalog-sql-small-test')
