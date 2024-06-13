import torch
from datasets import load_dataset
from transformers import (
    BitsAndBytesConfig,
    AutoModelForCausalLM, 
    AutoTokenizer,
    TrainingArguments,
    pipeline,
    logging
)
from peft import LoraConfig, PeftModel
from trl import SFTTrainer

DATASET_PATH = 'kaushikchan/catalog-sql-train'
#MODEL_PATH = 'meta-llama/CodeLlama-7b-hf-Instruct'
MODEL_PATH = 'meta-llama/Meta-Llama-3-8B-Instruct'
OUTPUT_DIR = './results'

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
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
tokenizer.add_special_tokens({'pad_token': '[PAD]'})
tokenizer.pad_token = tokenizer.eos_token
tokenizer.padding_side = 'right'

peft_config = LoraConfig(
    lora_alpha = 16,
    lora_dropout = 0.1,
    r = 64,
    bias = 'none',
    task_type = 'CAUSAL_LM'
)

training_arguments = TrainingArguments(
    output_dir = OUTPUT_DIR,
    num_train_epochs = 3,
    per_device_train_batch_size = 1,
    gradient_accumulation_steps = 1,
    optim = 'paged_adamw_32bit',
    save_steps = 25,
    logging_steps = 25,
    learning_rate = 2e-4,
    weight_decay = 0.01,
    fp16 = False,
    bf16 = False,
    max_grad_norm = 0.3,
    warmup_ratio = 0.03,
    group_by_length = True,
    lr_scheduler_type = 'constant',
    report_to = 'tensorboard'
)

trainer = SFTTrainer(
    model = model,
    train_dataset = dataset,
    peft_config = peft_config,
    dataset_text_field = 'text',
    max_seq_length = None,
    tokenizer = tokenizer,
    args = training_arguments,
    packing = False
)

trainer.train()
trainer.save_model(OUTPUT_DIR)
