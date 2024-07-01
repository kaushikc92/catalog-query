from unsloth import FastLanguageModel
import torch
import json
max_seq_length = 2048 
dtype = None 
load_in_4bit = True 

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = "lora_model",
    max_seq_length = max_seq_length,
    dtype = dtype,
    load_in_4bit = load_in_4bit,
)
FastLanguageModel.for_inference(model)

alpaca_prompt = """Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

### Instruction:
{}

### Input:
{}

### Response:
{}"""

EOS_TOKEN = tokenizer.eos_token 
def formatting_prompts_func(examples):
    instructions = examples["instruction"]
    inputs       = examples["input"]
    texts = []
    for instruction, input in zip(instructions, inputs):
        
        text = alpaca_prompt.format(instruction, input, "") + EOS_TOKEN
        texts.append(text)
    return { "text" : texts, }
pass

from datasets import load_dataset
dataset = load_dataset("kaushikchan/catalog-sql-test-alpaca", split = "train")
dataset = dataset.map(formatting_prompts_func, batched = True,)
dataset = dataset['text']
outputs = []
for d in dataset:
    inp = tokenizer([d], return_tensors = "pt").to("cuda")
    raw_output = model.generate(**inp, max_new_tokens = 64, use_cache = True)
    output = tokenizer.batch_decode(raw_output)
    outputs.append(output[0])
json.dump(outputs, open('answers.json', 'w'), indent=2)
