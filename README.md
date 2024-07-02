## Intall necessary python module
```
pip3 install -r requirements.txt
```

## For Pytorch and Unsloth
```
# INSTALL PYTORCH
pip install torch==2.3.0 torchvision==0.18.0 torchaudio==2.3.0 --index-url https://download.pytorch.org/whl/cu121

# INSTALL UNSLOTH
## this include bitsandbytes, xformers, trl, peft, transformers, accelerate
## For Pytorch 2.3.0: Use the "ampere" path for newer RTX 30xx GPUs or higher.
## Used in CSL - 2080 Ti
pip install "unsloth[cu121-torch230] @ git+https://github.com/unslothai/unsloth.git"

# Potential Errors:
# If error Cache only has 0 layers, attempted to access layer with index 0 shows up, redownload the transformer to older version
# https://github.com/unslothai/unsloth/issues/702
pip install transformers==4.38.0

# Other variants
<!-- pip install "unsloth[cu118-torch230] @ git+https://github.com/unslothai/unsloth.git"
pip install "unsloth[cu118-ampere-torch230] @ git+https://github.com/unslothai/unsloth.git"
pip install "unsloth[cu121-ampere-torch230] @ git+https://github.com/unslothai/unsloth.git" -->

# If not use unsloth:
python3 -m pip install --no-deps "xformers<0.0.26" trl peft accelerate bitsandbytes
```

## Post Processing
Note: It will fetch the table schema from postgres and create a file named **table_schema.json** if the file doesn't exist in the directory. If that's the case, remember to connect to the postgres server at first.

1. Check the sql query is syntactically correct
2. Check if the tables and columns are in the schema

```
from post_processor import SQLValidator
validator = SQLValidator(schema_match=True)
query = """
select t1.node_id, t1.short_name, sum(t3.fsize) from node_owner as t1 join edge_own as t2 on t1.node_id = t2.source_node_id join node_file as t3 on t2.target_node_id = t3.node_id group by t1.node_id, t1.short_name where t3.creation_date >= datetime('now', '-30 days');
"""
validator.validate_query(query)
```

