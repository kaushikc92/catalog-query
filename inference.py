import torch
from datasets import load_dataset
from transformers.pipelines.pt_utils import KeyDataset
from transformers import (
    BitsAndBytesConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    pipeline
)
import json
import time

DATASET_PATH = 'kaushikchan/catalog-sql-test'
MODEL_PATH = './results'

dataset = load_dataset(DATASET_PATH, split='train')

bnb_config = BitsAndBytesConfig(
    load_in_4bit = True,
    bnb_4bit_quant_type = 'nf4',
    bnb_4bit_compute_dtype = 'float16',
    bnb_4bit_use_double_quant = False
)

model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    quantization_config = bnb_config,
    device_map = 'auto',
    torch_dtype = torch.float16
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

