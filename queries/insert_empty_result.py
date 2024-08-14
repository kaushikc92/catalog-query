from datasets import load_dataset, Dataset
import pandas as pd
import psycopg
from psycopg.rows import dict_row
import sqlglot
from sqlglot import exp
import re
import json
import random
from datetime import datetime, timedelta
from faker import Faker

# Global Variable - File Path is required to be set
FAKER_GENERATOR= Faker()
NODE_TYPE_RECORDS_JSON_PATH = "../schema/node_type_records.json"
EDGE_TYPE_RECORDS_JSON_PATH = "../schema/edge_type_records.json"
TAXONOMY_JOIN_PATH = ['../schema/node_type_taxonomy.json','../schema/edge_type_taxonomy.json']
QUERY_PATH = 'queries.csv'

def check_time_stamp(expression):
    # Regular expression to match the pattern now() [+-] INTERVAL 'n' [HOURS|DAYS]
    pattern = r"(NOW\(\)\s*([+-])\s*INTERVAL\s*')(\d+)\s?(HOUR.*|DAY.*|MONTH.*|YEAR.*)"
    match = re.match(pattern, expression, re.IGNORECASE)
    if not match:
        return False
    return match

def replace_literal(node):
    if isinstance(node, exp.Interval):
        # Access the existing literal value and unit
        value = node.this
        unit = node.unit
        # Modify the literal value to include the unit
        new_literal_value = f"{value.this}{unit.this.lower()}"
        # Update the Interval node with the new literal
        new_interval = exp.Interval(
            this= exp.Literal(this=new_literal_value, is_string=True),
            unit=""  # Keep the unit unchanged
        )
        return new_interval
    return node

def modify_interval_value(expression, operation):
    match = check_time_stamp(expression)
    prefix = match.group(1)
    new_value = match.group(3)
    interval_type = match.group(4)
    # print(prefix, new_value, interval_type, sep='')
    if operation == '>':
        new_value = int(new_value) - min(3, int(new_value))
    elif operation == '<':
        new_value = int(new_value) + 3
    # Reconstruct the modified expression
    modified_expression = f"{prefix}{new_value}{interval_type}"
    return modified_expression

def analyze_literals(query):
    # Parse the SQL query
    expressions = sqlglot.parse_one(query)
    
    # Find all table references and where conditions
    where_conditions = expressions.find_all(sqlglot.expressions.Where)

    needed_inserts = {}
    alias_map = {}

    # Analyze tables and conditions
    for table in expressions.find_all(exp.Table):
        table_name = table.name
        alias = table.alias_or_name
        alias_map[alias] = table_name

    for table in expressions.find_all(exp.Table):
        table_name = table.args['this'].sql()
        if table_name not in needed_inserts:
            needed_inserts[table_name] = []
            
    for condition in where_conditions:
        condition = condition.transform(replace_literal)
        expr = condition.args['this']
        for condition in [sqlglot.expressions.EQ, sqlglot.expressions.LTE, sqlglot.expressions.GTE, sqlglot.expressions.LT, sqlglot.expressions.GT, sqlglot.expressions.Like]:
            comparisons = expr.find_all(condition)
            for comparison in comparisons:

                column = comparison.args['this'].this.sql() # remove the alias
                value = comparison.args['expression'].sql().strip('%')
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                
                if condition == sqlglot.expressions.LT or condition == sqlglot.expressions.LTE:
                    if check_time_stamp(value):
                        value = modify_interval_value(value, '<')
                    else:
                        value = str(int(value) - 1)
                elif condition == sqlglot.expressions.GT or condition == sqlglot.expressions.GTE:
                    if check_time_stamp(value):
                        value = modify_interval_value(value, '>')
                    else:
                        value = str(int(value) + 1)
                # if only one table, doesn't need to check it belongs to which table alias
                if comparison.this.table:
                    table_name = alias_map[comparison.this.table]
                needed_inserts[table_name].append((column, value))
        # print(needed_inserts)
    return needed_inserts

def find_same_value_attributes(joins):
    # Create a dictionary to map each attribute to its group
    attr_to_group = {}
    groups = []

    def get_group(attr):
        # If the attribute already has a group, return it
        if attr in attr_to_group:
            return attr_to_group[attr]
        # Otherwise, create a new group for this attribute
        new_group = [attr]
        groups.append(new_group)
        attr_to_group[attr] = new_group
        return new_group

    for join in joins:
        left_attr = (join['left_table'], join['left_column'])
        right_attr = (join['right_table'], join['right_column'])

        left_group = get_group(left_attr)
        right_group = get_group(right_attr)

        # If the left and right attributes are not already in the same group, merge the groups
        if left_group is not right_group:
            left_group.extend(right_group)
            for attr in right_group:
                attr_to_group[attr] = left_group
            groups.remove(right_group)

    return groups

def extract_joins(sql):
    # Parse the SQL query into an AST
    expression = sqlglot.parse_one(sql)
    join_info = []
    alias_map = {}

    # Extract table aliases
    for table in expression.find_all(exp.Table):
        table_name = table.name
        alias = table.alias_or_name
        alias_map[alias] = table_name

    def find_joins(node):
        if isinstance(node, exp.Join):
            # Extract the right table
            right_table_alias = node.this.alias_or_name
            right_table = alias_map.get(right_table_alias)

            # Extract the join condition (ON clause)
            condition = node.args.get('on')

            if condition:
                for condition_node in condition.find_all(exp.Condition):
                    # Extract the join attributes from the condition
                    left_column = condition_node.args.get('this')
                    right_column = condition_node.args.get('expression')
                    
                    if isinstance(left_column, exp.Column) and isinstance(right_column, exp.Column):
                        left_table_alias = left_column.args.get('table').this
                        left_table = alias_map.get(left_table_alias)
                        join_info.append({
                            'left_table': left_table,
                            'left_column': left_column.this.this,
                            'right_table': right_table,
                            'right_column': right_column.this.this
                        })
        return node

    # Traverse the AST and find joins
    expression.transform(find_joins)
    return find_same_value_attributes(join_info)

def get_taxonomy_info(taxonomy_json_path, child_node, path=None):
    if taxonomy_json_path is not None:
        taxonomy_json = {}
        for file_path in taxonomy_json_path:
            with open(file_path, 'r') as file:
                data = json.load(file)
                taxonomy_json.update(data)
    if path is None:
        path = []
    for key, values in taxonomy_json.items():
        if child_node in values and key not in path:
            path.append(key)
            get_taxonomy_info(taxonomy_json_path, key, path)            
    return path

def fetch_table_schema(postgreq_conn):
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
                        table_schema[current_table] = (current_table, columns)
                        columns = []
                    current_table = table_name
                columns.append(column_name)
            if current_table is not None:  
                table_schema[current_table] = (current_table, columns)
        return table_schema

def insert_data(postgres_conn, table_name, columns, data):
    with postgres_conn.cursor() as cur:
        # Prepare the columns and placeholders for the query
        placeholders = []
        column_headers = []

        for column, value in zip(columns, data):
            column_headers.append(column)
            if isinstance(value, str) and value.upper().startswith("NOW()"):
                placeholders.append(value)  # Directly add SQL expression
            else:
                placeholders.append('%s')  # Parameterized placeholder

        placeholders_str = ', '.join(placeholders)
        column_headers_str = ', '.join(column_headers)
        sql_query = f"INSERT INTO {table_name} ({column_headers_str}) VALUES ({placeholders_str})"
        data_filtered = [value for value in data if not (isinstance(value, str) and value.upper().startswith("NOW()"))]
        
        # Execute the query
        cur.execute(sql_query, data_filtered)
    
    postgres_conn.commit()


def fetch_id(postgreq_conn):
    try:
        # Create a cursor object
        cursor = postgreq_conn.cursor() 
        # Query to fetch the largest node_id from the node table
        cursor.execute("SELECT MAX(node_id) FROM node")
        largest_node_id = cursor.fetchone()[0]
        
        # Query to fetch the largest edge_id from the edge table
        cursor.execute("SELECT MAX(edge_id) FROM edge")
        largest_edge_id = cursor.fetchone()[0]
        
        # Close the cursor and connection
        cursor.close()
        return largest_node_id, largest_edge_id

    except psycopg.Error as e:
        print(f"Error fetching largest IDs: {e}")
        return None, None

def generate_random_date(start_year=1970, end_year=datetime.now().year):
    # Generate a random date between start_year and end_year
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime("%c")

def insert_tuples(postgresq_conn, taxonomy_json_path, missed_data, join_data):
    # Iterate over the data to insert
    data_catalog = fetch_table_schema(postgresq_conn)
    data_multi_table = {}

    potential_join_tables = []
    for group in join_data:
        for table_name, column_name in group:
            potential_join_tables.append(table_name)
         
    # fetch the largest node_id or edge_id
    largest_node_id, largest_edge_id = fetch_id(postgresq_conn)

    for table, columns in data_catalog.values():
        if table not in missed_data.keys() and table not in potential_join_tables:
            continue
        elif table in missed_data.keys() and table not in potential_join_tables and not missed_data[table]:
            continue

        # Build Data
        data = {column: None for column in columns}
        for key, value in data.items():
            if "date" in key.lower():
                data[key] = generate_random_date()
            elif "num" in key.lower() or "length" in key.lower() or "size" in key.lower() or "id" in key.lower():
                data[key] = random.randint(1, 1000)
            else:
                # word_list = words.words()
                length = random.randint(1, 5)
                random_words = [FAKER_GENERATOR.word() for _ in range(length)]
                data[key] = ' '.join(random_words)
                if 'short_name' in key.lower():
                    data[key] = '_'.join(random_words).lower()

        # load the type_id
        node_type_schema = json.load(open(NODE_TYPE_RECORDS_JSON_PATH))
        edge_type_schema = json.load(open(EDGE_TYPE_RECORDS_JSON_PATH))

        if 'node' in table.lower():
            data['node_id'] = largest_node_id + 1
            data['type_id'] = node_type_schema[table]['type_id']
            largest_node_id += 1 
        elif 'edge' in table.lower():
            data['edge_id'] = largest_edge_id + 1
            data['type_id'] = edge_type_schema[table]['type_id']
            largest_edge_id += 1
            
        underscore_position = table.find('_')
        # Fetch the string after the first underscore
        if underscore_position != -1:
            data['type_name'] = table[underscore_position + 1:]
        else:
            data['type_name'] = table  # In case there is no underscore in the string
        # update the value
        if table in missed_data.keys():
            for column, value in missed_data[table]:
                data[column] = value
        data_multi_table[table] = data
    #print(data_multi_table)

    for group in join_data:
        for table_name, column_name in group:
            if column_name == 'edge_id' or column_name == 'node_id':
                found_id = True
                for table_name_2, column_name_2 in group:
                    data_multi_table[table_name_2][column_name_2] = data_multi_table[table_name][column_name]
                break
        if not found_id:
            for table_name, column_name in group:
                data_multi_table[table_name][column_name] = data_multi_table[group[0][0]][group[0][1]]
                if column_name == 'type_id':
                    data_multi_table[table_name]['type_name'] = data_multi_table[group[0][0]]['type_name']
                if column_name == 'type_name':
                    data_multi_table[table_name]['type_id'] = data_multi_table[group[0][0]]['type_id']
    # print(data_multi_table)      

    for table, data in data_multi_table.items():
        columns = data.keys()
        value_list = [data.get(column) for column in columns]
        # print(table, columns, value_list, sep='\n')
        insert_data(postgresq_conn, table, columns, value_list)
        # print(f"Data inserted into table {table}.")
        # print()
        # Handle taxonomy information
        if taxonomy_json_path is not None:
            for parent in get_taxonomy_info(taxonomy_json_path, table):
                parent_columns = data_catalog[parent][1]
                parent_value_list = [data.get(column) for column in parent_columns]
                insert_data(postgresq_conn, parent, parent_columns, parent_value_list)
                # print(parent, parent_columns, parent_value_list, sep='\n')
                # print(f"Data inserted into table {parent}.")
                # print()

def analyze_query_insert(query, postgreq_conn):

    # Extract join details and literals
    join_details = extract_joins(query)
    missed_data = analyze_literals(query)

    # insert the data
    insert_tuples(postgreq_conn, TAXONOMY_JOIN_PATH, missed_data, join_details)

def fix_empty_queries(df, postgreq_params):
    empty_query = 0
    for index, row in df.iterrows():
        with psycopg.connect(**postgreq_params) as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Process the gold standard query
                try:
                    cursor.execute(row['goldSqlQuery'])
                    gold_results = cursor.fetchall()
                    if not gold_results:
                        # print(row['goldSqlQuery'])
                        # print("gold query is empty")
                        analyze_query_insert(row['goldSqlQuery'], conn)
                except psycopg.Error as e:
                    pass
                    # print("Gold query can not be runned.\nError:", e)

def check_empty_queries(df, postgreq_params):
    empty_query = 0
    can_not_run = 0
    empty_query_dict = []
    can_not_run_dict = []
    for index, row in df.iterrows():
        with psycopg.connect(**postgreq_params) as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                # Process the gold standard query
                try:
                    cursor.execute(row['goldSqlQuery'])
                    gold_results = cursor.fetchall()
                    if gold_results:
                        # print("gold query is not empty")
                        pass
                    else:
                        # print(row['eid'])
                        # print(row['goldSqlQuery'])
                        # print("gold query is empty")
                        empty_query_dict.append({"eid": row['eid'],"goldSqlQuery":row['goldSqlQuery']} )
                        empty_query += 1
                except psycopg.Error as e:
                    can_not_run += 1
                    can_not_run_dict.append({"eid": row['eid'],"goldSqlQuery":row['goldSqlQuery']})
                    # print(row['eid'])
                    # print(row['goldSqlQuery'])
                    # print("Gold query can not be runned.\nError:", e)
    print("Statistics:")
    print("Empty queries:", empty_query)
    print("Can not run queries:", can_not_run)
    return empty_query, can_not_run, empty_query_dict, can_not_run_dict

# query_dataset = load_dataset("json", data_files=QUERY_PATH, split='train')
# df = pd.DataFrame(query_dataset)
df = pd.read_csv(QUERY_PATH)
df = df.rename(columns={'example_id': 'eid', 'gold_query': 'goldSqlQuery'})

# Connection setup
# conn = psycopg.connect("dbname=testdb user=yourusername password=yourpassword")

postgreq_params = {
        "host": "localhost",
        "dbname": "data_catalog",
        "user": "text2sql"
    }
print("Before Fixing:")
check_empty_queries(df, postgreq_params)
print()

fix_empty_queries(df, postgreq_params)

print("After Fixing:")
final_result = check_empty_queries(df, postgreq_params)
json.dump(final_result[2], open('empty_queries.json', 'w'), indent=4)
json.dump(final_result[3], open('can_not_run_queries.json', 'w'), indent=4)
