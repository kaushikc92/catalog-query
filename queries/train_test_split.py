from datasets import load_dataset
import json

N_TRAIN = 500
N_TEST = 700
QUERY_PATH = './queries.json'

query_dataset = load_dataset("json", data_files=QUERY_PATH, split='train')
query_dataset = query_dataset.train_test_split(train_size=N_TRAIN, test_size=N_TEST, seed=10086)

train_data = query_dataset['train'].to_pandas()
test_data = query_dataset['test'].to_pandas()

# Save train and test data as JSON files
train_data.to_json("train.json", orient='records', indent=2)
test_data.to_json("test.json", orient='records', indent=2)
