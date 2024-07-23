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
        
        text = alpaca_prompt.format(instruction, input, "")
        texts.append(text)
    return { "text" : texts, }
pass

from datasets import load_dataset
dataset = load_dataset("kaushikchan/catalog-sql-test-alpaca", split = "train")
dataset = dataset.map(formatting_prompts_func, batched = True,)
dataset = dataset['text']
outputs = []

batch_size = 16
final_text = []
for i in range(0, len(dataset), batch_size):
    batch = dataset[i:i+batch_size]
    inp = tokenizer(batch, return_tensors = "pt", padding = True).to("cuda")
    raw_output = model.generate(**inp, max_new_tokens = 200, use_cache = True)
    output = tokenizer.batch_decode(raw_output[:, inp["input_ids"].shape[1]:], skip_special_tokens=True)
    for original, generated in zip(batch, output):
        final_text.append(original + generated) # Because the tokenizer is lossy, it's better to keep the same prompt.
# for d in dataset:
#     inp = tokenizer([d], return_tensors = "pt").to("cuda")
#     raw_output = model.generate(**inp, max_new_tokens = 64, use_cache = True)
#     output = tokenizer.batch_decode(raw_output)
#     outputs.append(output[0])
json.dump(final_text, open('answers.json', 'w'), indent=2)
