import psycopg
import json
import os

def fetch_table_schema():
    postgreq_params = {
        "host": "localhost",
        "dbname": "data_catalog",
        "user": "text2sql"
    }
    postgreq_conn = psycopg.connect(**postgreq_params)
    table_schema = {}
    query = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """
    with postgreq_conn.cursor() as cur:
        cur.execute(query)
        current_table = None
        columns = []
        for row in cur.fetchall():
            table_name, column_name = row
            if table_name != current_table:
                if current_table is not None:
                    table_schema[current_table] = columns
                    columns = []
                current_table = table_name
            columns.append(column_name)
        if current_table is not None:  
            table_schema[current_table] = columns
    return table_schema

## two candidiates
# https://github.com/sqlfluff/sqlfluff
# https://github.com/tobymao/sqlglot
import sqlglot
from sqlglot.expressions import Column, Table

def is_valid_sql(query):
    try:
        # Attempt to parse the SQL query
        parsed_query = sqlglot.parse_one(query, dialect="postgres")
        return True, parsed_query # Parsing successful, SQL is valid
    except sqlglot.errors.ParseError as e:
        # Parsing failed, SQL is invalid
        # print(f"Invalid SQL: {e}")
        return False, None
    
# Function to map columns to their respective tables
def map_columns_to_tables(parsed_query):
    table_alias_map = {}
    column_table_map = {}
    unambiguous_columns = []

    # First pass: find all table references and their aliases (if exists)
    for table in parsed_query.find_all(Table):
        table_name = table.name
        alias = table.alias_or_name
        table_alias_map[alias] = table_name

    # Second pass: find all columns and map them to their respective tables
    for column in parsed_query.find_all(Column):
        table_alias = column.table
        column_name = column.name
        if table_alias:
            table_name = table_alias_map.get(table_alias, table_alias)
            if table_name not in column_table_map:
                column_table_map[table_name] = []
            column_table_map[table_name].append(column_name)
        else:
            unambiguous_columns.append(column_name)

    # If there is only one table, assume unambiguous columns belong to that table
    if len(table_alias_map) == 1:
        table_name = next(iter(table_alias_map.values()))
        if table_name not in column_table_map:
            column_table_map[table_name] = []
        column_table_map[table_name].extend(unambiguous_columns)
    else:
        # Handle columns without explicit table reference (e.g., unambiguous columns)
        if len(unambiguous_columns) > 0:
            column_table_map[None] = unambiguous_columns

    return column_table_map

def query_validator(query, schema = None):
    ## check if the sql is syntactically correct
    valid_sql, parsed_sql = is_valid_sql(query)
    if valid_sql:
        print("Query is valid.")
        ## check if the tables and columns are in the schema
        column_table_map = map_columns_to_tables(parsed_sql)

        # Print the results
        for table, columns in column_table_map.items():
            print(f"Table: {table}, Columns: {columns}")

        if schema is not None:
            for table, columns in column_table_map.items():
                if table not in schema:
                    print(f"Error: Table '{table}' is not in the schema.")
                    return False
                for column in columns:
                    if column not in schema[table]:
                        print(f"Error: Column '{column}' is not in the schema for table '{table}'.")
                        return False
        print("Query conforms to schema.")
        return True
    else:
        return False

schema_file = 'table_schema.json'
if not os.path.exists(schema_file):
    with open(schema_file, 'w') as file:
        json.dump(fetch_table_schema(), file, indent=4)
else:
    with open(schema_file, 'r') as file:
        schema = json.load(file)

query = """
select t1.node_id, t1.short_name, sum(t3.fsize) from node_owner as t1 join edge_own as t2 on t1.node_id = t2.source_node_id join node_file as t3 on t2.target_node_id = t3.node_id group by t1.node_id, t1.short_name where t3.creation_date >= datetime('now', '-30 days');
"""
with open('table_schema.json', 'r') as file:
    schema = json.load(file)
print(query_validator(query, schema))
