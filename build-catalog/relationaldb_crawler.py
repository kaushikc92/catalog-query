import psycopg
from neo4j import GraphDatabase
import json

class PostgresManager:
    def __init__(self, postgreq_params, neo4j_params, taxonomy_json_path = None):
        # Initialize PostgreSQL connection
        self.postgreq_conn = psycopg.connect(**postgreq_params)
        # Initialize Neo4j connection
        self.neo4j_driver = GraphDatabase.driver(neo4j_params['uri'], auth=(neo4j_params['user'], neo4j_params['password']))
        # Load taxonomy JSON
        self.taxonomy_json_path = taxonomy_json_path
        if taxonomy_json_path is not None:
            combined_data = {}
            for file_path in taxonomy_json_path:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    combined_data.update(data)
            self.taxonomy_json = combined_data
    
    def drop_tables(self):
        """Drops all tables in the 'public' schema of the database."""
        with self.postgreq_conn.cursor() as cur:
            sql = """
            DO
            $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END;
            $$;
            """
            cur.execute(sql)
            self.postgreq_conn.commit()
            print("All tables in the 'public' schema have been dropped.")
    
    def create_tables(self, sql_commands):
        """Creates tables in the database using the provided SQL commands."""
        with self.postgreq_conn.cursor() as cur:
            print("Connected to the PostgreSQL database.")
            for command in sql_commands:
                cur.execute(command)
                print(f"Executed: {command}")
            self.postgreq_conn.commit()
            print("All tables created successfully.")
    
    def fetch_data(self, label):
        with self.neo4j_driver.session() as session:
            if label.startswith("node"):
                result = session.run(f"MATCH (n:{label}) RETURN n")
                return [record["n"] for record in result]
            elif label.startswith("edge"):
                result = session.run(f"MATCH p=()-[r:{label}]->() RETURN r")
                return [record["r"] for record in result]
    
    def insert_data(self, table_name, columns, data):
        with self.postgreq_conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(columns))
            column_headers = ', '.join(columns)           
            cur.execute(f"INSERT INTO {table_name} ({column_headers}) VALUES ({placeholders})", data)
        self.postgreq_conn.commit()

    def fetch_table_schema(self):
        table_schema = {}
        query = """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """
        with self.postgreq_conn.cursor() as cur:
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

    def get_taxonomy_info(self, child_node, path=None):
        if path is None:
            path = []
        for key, values in self.taxonomy_json.items():
            if child_node in values and key not in path:
                path.append(key)
                self.get_taxonomy_info(key, path)            
        return path

    def synchronize_data(self):
        """
        Synchronize data from Neo4j to PostgreSQL based on defined taxonomy.
        """
        # Fetch table schema
        data_catalog = self.fetch_table_schema()
        # Synchronize data
        for table, columns in data_catalog.values():
            if table in self.taxonomy_json:  # Skip if table is a non-leaf node in taxonomy
                continue
            data = self.fetch_data(table)
            for values in data:
                value_list = [values.get(column) for column in columns]
                self.insert_data(table, columns, value_list)
                print(f"Data inserted into table {table}.")
            # Handle taxonomy information
            if self.taxonomy_json_path is not None:
                for parent in self.get_taxonomy_info(table):
                    parent_columns = data_catalog[parent][1]
                    for values in data:
                        parent_value_list = [values.get(column) for column in parent_columns]
                        self.insert_data(parent, parent_columns, parent_value_list)
                        print(f"Data inserted into table {parent}.")
        print("All data transferred successfully.")

    def close_connections(self):
        """Closes all database connections."""
        self.postgreq_conn.close()
        self.neo4j_driver.close()

# Usage example
if __name__ == "__main__":
    neo4j_params = {
        "uri": "neo4j://localhost:7687",
        "user": "neo4j",
        "password": "12345678"
    }
    postgreq_params = {
        "host": "localhost",
        "dbname": "data_catalog",
        "user": "text2sql"
    }
    # SQL statements for creating tables
    sql_node_commands = [
    "CREATE TABLE node (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE node_directory (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, dsize FLOAT);",
    "CREATE TABLE node_file (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, extension TEXT, fsize FLOAT);",
    "CREATE TABLE node_table (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, num_cols INT, num_rows INT);",
    "CREATE TABLE node_column (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, col_type TEXT, max_col_length INT);",
    "CREATE TABLE node_database (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, database_type TEXT);",
    "CREATE TABLE node_rdbms (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP, num_tables INT);",
    "CREATE TABLE node_nosql (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE node_label (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE node_business_term (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE node_classification (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE node_owner (node_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, creation_date TIMESTAMP, modified_date TIMESTAMP);"
    ]
    sql_edge_commands = [
    "CREATE TABLE edge (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_has (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_has_dir_dir (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_has_dir_file (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_has_file_table (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_has_table_col (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_assoc (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_assoc_term_col (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_assoc_class_col (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_derive_table_table (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_joinable_table_table (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_unionable_table_table (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);",
    "CREATE TABLE edge_own (edge_id SERIAL PRIMARY KEY, type_id INT, short_name TEXT, long_name TEXT, description TEXT, source_node_id INT, target_node_id INT, creation_date TIMESTAMP, modified_date TIMESTAMP);"
    ]
    db_manager = PostgresManager(postgreq_params, neo4j_params, taxonomy_json_path = ["../schema/node_type_taxonomy.json","../schema/edge_type_taxonomy.json"])
    db_manager.create_tables(sql_node_commands + sql_edge_commands)
    db_manager.synchronize_data()
    #db_manager.drop_tables()
    db_manager.close_connections()