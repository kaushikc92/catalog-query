import psycopg
import json
import os
import sqlglot
from sqlglot.expressions import Column, Table

## two candidiates
# https://github.com/sqlfluff/sqlfluff
# https://github.com/tobymao/sqlglot
class SQLValidator:
    def __init__(self, schema_match=True):
        self.schema_file = "./schema/sql/schema.json"
        if schema_match == True:
            self.schema = self.load_or_fetch_schema()
        else:
            self.schema = None

    def fetch_table_schema(self):
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

    def load_or_fetch_schema(self):
        if not os.path.exists(self.schema_file):
            schema = self.fetch_table_schema()
            with open(self.schema_file, 'w') as file:
                json.dump(schema, file, indent=2)
            return schema
        else:
            with open(self.schema_file, 'r') as file:
                return json.load(file)

    def is_valid_sql(self, query):
        try:
            parsed_query = sqlglot.parse_one(query, dialect="postgres")
            return True, parsed_query
        except sqlglot.errors.SqlglotError as e:
            # Parsing failed, SQL is invalid
            # print(f"Invalid SQL: {e}")
            return False, None

    def map_columns_to_tables(self, parsed_query):
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


    def validate_query(self, query):
        ## check if the sql is syntactically correct
        valid_sql, parsed_sql = self.is_valid_sql(query)
        if valid_sql:
            print("Query is valid.")
            ## check if the tables and columns are in the schema
            column_table_map = self.map_columns_to_tables(parsed_sql)

            # Print the results
            for table, columns in column_table_map.items():
                print(f"Table: {table}, Columns: {columns}")

            if self.schema is not None:
                for table, columns in column_table_map.items():
                    if table not in self.schema:
                        print(f"Error: Table '{table}' is not in the schema.")
                        return False
                    for column in columns:
                        if column not in self.schema[table]:
                            print(f"Error: Column '{column}' is not in the schema for table '{table}'.")
                            return False
            print("Query conforms to schema.")
            return True
        else:
            return False