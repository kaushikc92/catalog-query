python3 -m pip install --no-deps "xformers<0.0.26" trl peft accelerate bitsandbytes

## Intall necessary python module
```
pip3 install -r ../requirements.txt
```

## Post Processing
Note: It will fetch the table schema from postgres and create a file named **table_schema.json** if the file doesn't exist in the directory. If that's the case, remember to connect to the postgres server at first.

1. Check the sql query is syntactically correct
2. Check if the tables and columns are in the schema

