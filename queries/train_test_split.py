from datasets import load_dataset
import json
import random
import numpy as np

# Set the seed for reproducibility
SEED = 10086
random.seed(SEED)

N_TRAIN = 500
N_TEST = 700
QUERY_PATH = './queries.json'
SCHEMA_PATH = '../schema/sql/schema.json'

schema = json.load(open(SCHEMA_PATH, 'r'))
query_dataset = load_dataset("json", data_files=QUERY_PATH, split='train')
query_dataset = query_dataset.train_test_split(train_size=N_TRAIN, test_size=N_TEST, seed=SEED)

train_data = query_dataset['train'].to_pandas()
test_data = query_dataset['test'].to_pandas()

train_data.insert(6, 'training_tables', None)
train_data.insert(7, 'training_synonyms', None)

for index, query in train_data.iterrows():
    tables = query['tables'].tolist()
    n_tables = random.randint(5, 10)
    while len(tables) < n_tables:
        table = random.choice(list(schema.keys()))
        if table not in tables:
            tables.append(table)
    random.shuffle(tables)
    train_data.at[index, 'training_tables'] = tables
    concatenated = np.concatenate((query['synonyms_from_exact_match'], query['synonyms_from_embeddings']))
    synonyms_as_tuples = [tuple(syn) for syn in concatenated]
    train_data.at[index, 'training_synonyms'] = list(set(synonyms_as_tuples))
train_data = train_data.drop(columns=['tables_from_tfidf', 'tables_from_embeddings'])

test_data.insert(6, 'testing_tables', None)
test_data.insert(7, 'testing_synonyms', None)
for index, query in test_data.iterrows():
    concatenated = np.concatenate((query['tables_from_tfidf'], query['tables_from_embeddings']))
    tables_as_tuples = [tuple(syn) for syn in concatenated]
    test_data.at[index, 'testing_tables'] = np.unique(np.concatenate((query['tables_from_tfidf'], query['tables_from_embeddings'])))
    concatenated = np.concatenate((query['synonyms_from_exact_match'], query['synonyms_from_embeddings']))
    synonyms_as_tuples = [tuple(syn) for syn in concatenated]
    test_data.at[index, 'testing_synonyms'] = list(set(synonyms_as_tuples))
# Save train and test data as JSON files
train_data.to_json("train.json", orient='records', indent=2)
test_data.to_json("test.json", orient='records', indent=2)
