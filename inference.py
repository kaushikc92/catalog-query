import torch
from datasets import load_dataset
from transformers.pipelines.pt_utils import KeyDataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    pipeline
)
import json
import time

DATASET_PATH = 'kaushikchan/catalog-sql-small-test'
MODEL_PATH = './results'

dataset = load_dataset(DATASET_PATH, split='train')

model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    device_map = 'auto'
)
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)

pipe = pipeline(task='text-generation', model=model, tokenizer=tokenizer)

answers = []

for out in pipe(KeyDataset(dataset, "text"), max_new_tokens=100):
    start_time = time.time()
    answers.append(out[0]['generated_text'])
    json.dump(answers, open('answers.json', 'w'), indent=2)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
    break

