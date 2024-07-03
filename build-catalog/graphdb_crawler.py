from neo4j import GraphDatabase
import os
import time
#from uuid import uuid4
import subprocess
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import random
import json
from camelcase_tokenizer import CamelCaseTokenizer
import pwd

class Neo4jManager:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.node_type_schema = json.load(open("../schema/node_type_records.json"))
        self.edge_type_schema = json.load(open("../schema/edge_type_records.json"))
        self.tokenizer = CamelCaseTokenizer()
        self.node_id = 0
        self.edge_id = 0

    def close(self):
        self.driver.close()

    def create_nodes(self, path, type, parameters = None):
        with self.driver.session() as session:
            result =  session.execute_write(self._create_nodes, path, type, parameters)
            self.node_id += 1
            return result

    def create_edges(self, source_node, target_node, edge_type):
        with self.driver.session() as session:
            result = session.execute_write(self._create_edges, source_node, target_node, edge_type)
            self.edge_id += 1
            return result
    
    def clean_path(self,path):
        if len(os.path.basename(path)) == 0:
            return path.split("/")[2]
        else:
            return os.path.basename(path)
    
    def get_file_owner(self,file_path):
        try:
            # Get the user ID of the file
            stat_info = os.stat(file_path)
            uid = stat_info.st_uid
            # Get the username from the user ID
            user_info = pwd.getpwuid(uid)
            return user_info.pw_name
        except Exception as e:
            return str(e)

    ## only works on linux
    # def get_directory_size(self, directory):
    #     result = subprocess.run(['du', '-s', '-h', directory], stdout=subprocess.PIPE, text=True)
    #     size, _ = result.stdout.split()
    #     return size  

    def get_directory_size(self, directory):
        total = 0
        with os.scandir(directory) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += self.get_directory_size(entry.path)
        total = total / 1024 # in KB
        return total
    
    def _create_nodes(self, tx, path, node_type, parameters):
        # node_id = str(uuid4())
        type_id = self.node_type_schema["node_" + node_type]['type_id'] 
        description = ""
        if node_type == "directory":
            temp_name = self.clean_path(path)
            short_name = "_".join(self.tokenizer.tokenize(temp_name)).lower()
            long_name = " ".join(self.tokenizer.tokenize(temp_name)) #TODO: Give the real long name later
            creation_date = time.strftime("%c", time.gmtime(os.path.getctime(path)))
            modified_date = time.strftime("%c", time.gmtime(os.path.getmtime(path)))
            dsize = self.get_directory_size(path)
            tx.run("""
                MERGE (f:node_directory {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    dsize: $dsize
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, dsize=dsize)        
        elif node_type == "file":
            temp_name = self.clean_path(path)
            short_name = "_".join(self.tokenizer.tokenize(temp_name)).lower()
            long_name = " ".join(self.tokenizer.tokenize(temp_name))
            creation_date = time.strftime("%c", time.gmtime(os.path.getctime(path)))
            modified_date = time.strftime("%c", time.gmtime(os.path.getmtime(path)))
            extension = os.path.splitext(path)[1]
            size_bytes = os.path.getsize(path)  # in Bytes
            # suffixes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
            # # Convert size to a more readable format
            # i = 0
            # while size_bytes >= 1024 and i < len(suffixes)-1:
            #     size_bytes /= 1024.
            #     i += 1
            # fsize = f"{size_bytes:.2f} {suffixes[i]}"
            fsize = size_bytes / 1024 # in KB
            tx.run("""
                MERGE (f:node_file {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    extension: $extension,
                    fsize: $fsize
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, extension=extension, fsize=fsize)            
        elif node_type == "table":
            temp_name = parameters["table_name"]
            short_name = "_".join(self.tokenizer.tokenize(temp_name)).lower()
            long_name = " ".join(self.tokenizer.tokenize(temp_name))
            creation_date = time.strftime("%c", time.gmtime())
            modified_date = time.strftime("%c", time.gmtime())
            num_cols = parameters["num_cols"]
            num_rows = parameters["num_rows"]
            tx.run("""
                MERGE (f:node_table {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    num_cols: $num_cols,
                    num_rows: $num_rows
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, num_cols=num_cols, num_rows=num_rows)        
        elif node_type == "column":
            temp_name = parameters["column_name"]
            short_name = "_".join(self.tokenizer.tokenize(temp_name)).lower()
            long_name = " ".join(self.tokenizer.tokenize(temp_name))
            creation_date = time.strftime("%c", time.gmtime())
            modified_date = time.strftime("%c", time.gmtime())
            col_type = parameters["col_type"]
            max_col_length = parameters["max_col_length"]
            tx.run("""
                MERGE (f:node_column {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    col_type: $col_type,
                    max_col_length: $max_col_length
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, col_type=col_type, max_col_length=max_col_length)            
        elif node_type == "database":
            database_type = parameters["database_type"]
            tx.run("""
                MERGE (f:node_database {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    database_type: $database_type
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, database_type=database_type)   
        elif node_type == "rdbms":
            num_tables = parameters["num_tables"]
            tx.run("""
                MERGE (f:node_rdbms {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date,
                    num_tables: $num_tables
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date, num_tables=num_tables)
        elif node_type == "nosql":
            tx.run("""
                MERGE (f:node_nosql {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        elif node_type == "label":
            tx.run("""
                MERGE (f:node_label {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        elif node_type == "business_term":
            tx.run("""
                MERGE (f:node_business_term {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        elif node_type == "classification":
            tx.run("""
                MERGE (f:node_classification {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        elif node_type == "owner":
            temp_name = parameters["owner_name"]
            short_name = "_".join(self.tokenizer.tokenize(temp_name)).lower()
            long_name = " ".join(self.tokenizer.tokenize(temp_name))
            creation_date = time.strftime("%c", time.gmtime())
            modified_date = time.strftime("%c", time.gmtime())
            tx.run("""
                MERGE (f:node_owner {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        else:
            tx.run("""
                MERGE (f:node {
                    node_id: $node_id,
                    type_id: $type_id,
                    short_name: $short_name,
                    long_name: $long_name,
                    description: $description,
                    creation_date: $creation_date,
                    modified_date: $modified_date
                })
                """, node_id=self.node_id, type_id=type_id,
                short_name=short_name, long_name=long_name, description=description,
                creation_date=creation_date, modified_date=modified_date)
        return self.node_id

           
                    
    def _create_edges(self, tx, source_node, target_node, edge_type):
        # Assigning unique UUID and type_id for files
        #edge_id = str(uuid4())
        edge_id = self.edge_id
        type_id = self.edge_type_schema[edge_type]['type_id']  # Example type_id for files
        short_name = ""
        long_name = ""
        description = ""
        source_node_id = source_node
        target_node_id = target_node
        creation_date = time.strftime("%c", time.gmtime())
        modified_date = time.strftime("%c", time.gmtime())
        
        if edge_type == "edge_has_dir_dir":
            tx.run("""
                MATCH (src:node_directory {node_id: $source_node_id}), (tgt:node_directory {node_id: $target_node_id})
                MERGE (src)-[r:edge_has_dir_dir]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_has_dir_file":
            tx.run("""
                MATCH (src:node_directory {node_id: $source_node_id}), (tgt:node_file {node_id: $target_node_id})
                MERGE (src)-[r:edge_has_dir_file]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_has_file_table":
            tx.run("""
                MATCH (src:node_file {node_id: $source_node_id}), (tgt:node_table {node_id: $target_node_id})
                MERGE (src)-[r:edge_has_file_table]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_has_table_col":
            tx.run("""
                MATCH (src:node_table {node_id: $source_node_id}), (tgt:node_column {node_id: $target_node_id})
                MERGE (src)-[r:edge_has_table_col]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_assoc_term_col":
            tx.run("""
                MATCH (src:node_column {node_id: $source_node_id}), (tgt:node_business_term {node_id: $target_node_id})
                MERGE (src)-[r:edge_assoc_term_col]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_assoc_class_col":
            tx.run("""
                MATCH (src:node_column {node_id: $source_node_id}), (tgt:node_classification {node_id: $target_node_id})
                MERGE (src)-[r:edge_assoc_class_col]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_derive_table_table":
            tx.run("""
                MATCH (src:node_table {node_id: $source_node_id}), (tgt:node_table {node_id: $target_node_id})
                MERGE (src)-[r:edge_derive_table_table]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_joinable_table_table":
            tx.run("""
                MATCH (src:node_table {node_id: $source_node_id}), (tgt:node_table {node_id: $target_node_id})
                MERGE (src)-[r:edge_derive_table_table]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_unionable_table_table":
            tx.run("""
                MATCH (src:node_table {node_id: $source_node_id}), (tgt:node_table {node_id: $target_node_id})
                MERGE (src)-[r:edge_unionable_table_table]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        elif edge_type == "edge_own":
            tx.run("""
                MATCH (src:node_owner {node_id: $source_node_id}), (tgt {node_id: $target_node_id})
                MERGE (src)-[r:edge_own]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)
        else:
            tx.run("""
                MATCH (src:node {node_id: $source_node_id}), (tgt:node {node_id: $target_node_id})
                MERGE (src)-[r:edge]->(tgt)
                SET r.edge_id = $edge_id,
                    r.type_id = $type_id,
                    r.short_name = $short_name,
                    r.long_name = $long_name,
                    r.description = $description,
                    r.source_node_id = $source_node_id,
                    r.target_node_id = $target_node_id,
                    r.creation_date = $creation_date,
                    r.modified_date = $modified_date
                """, source_node_id=source_node_id, target_node_id=target_node_id,
                edge_id=edge_id, type_id=type_id, short_name=short_name, long_name=long_name,
                description=description, creation_date=creation_date, modified_date=modified_date)

        
    def traverse_directory(self, directory_path):
        for root, dirs, files in os.walk(directory_path):
            root_id = self.create_nodes(root, "directory")
            owner_id = self.create_nodes(self.get_file_owner(root), "owner", parameters={"owner_name": self.get_file_owner(root)})
            self.create_edges(owner_id, root_id, "edge_own")
            for file in files:
                file_id = self.create_nodes(root + "/" + file, "file")
                self.create_edges(root_id, file_id, "edge_has_dir_file")
                owner_id = self.create_nodes(self.get_file_owner(root + "/" + file), "owner", parameters={"owner_name": self.get_file_owner(root + "/" + file)})
                self.create_edges(owner_id, file_id, "edge_own")
                if file.split(".")[-1] in ["csv", "json"]: #TODO: Add more file types
                    self.process_file(root + "/" + file, file_id, file.split(".")[-1])
            for dir in dirs:
                dir_id = self.create_nodes(root + "/" + dir, "directory")
                self.create_edges(root_id, dir_id, "edge_has_dir_dir")
                owner_id = self.create_nodes(self.get_file_owner(root + "/" + dir), "owner", parameters={"owner_name": self.get_file_owner(root + "/" + dir)})
                self.create_edges(owner_id, dir_id, "edge_own")
                self.traverse_sub_directory(root + "/" + dir, dir_id)
            break
    
    def traverse_sub_directory(self, directory_path, root_id):
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_id = self.create_nodes(root + "/" + file, "file")
                self.create_edges(root_id, file_id, "edge_has_dir_file")
                owner_id = self.create_nodes(self.get_file_owner(root + "/" + file), "owner", parameters={"owner_name": self.get_file_owner(root + "/" + file)})
                self.create_edges(owner_id, file_id, "edge_own")
                if file.split(".")[-1] in ["csv", "json"]: #TODO: Add more file types
                    self.process_file(root + "/" + file, file_id, file.split(".")[-1])
            for dir in dirs:
                dir_id = self.create_nodes(root + "/" + dir, "directory")
                self.create_edges(root_id, dir_id, "edge_has_dir_dir")
                owner_id = self.create_nodes(self.get_file_owner(root + "/" + dir), "owner", parameters={"owner_name": self.get_file_owner(root + "/" + dir)})
                self.create_edges(owner_id, dir_id, "edge_own")
                self.traverse_sub_directory(root + "/" + dir, dir_id)
            break
    
    def process_file(self, file_path, file_id, type):
        """ Read a file using PyArrow and print its details along with column data types. """
        try:
            # Read the file into a PyArrow Table
            if type == "csv":
                table = pa_csv.read_csv(file_path)
            elif type == "json":
                table = pa_json.read_json(file_path)
            # Extract table name from the file path
            table_name = '.'.join(os.path.basename(file_path).split('.')[:-1])
            parameters = {"table_name": table_name, "num_rows": table.num_rows, "num_cols": table.num_columns}
            table_id = self.create_nodes(table_name, "table", parameters)
        
            self.create_edges(file_id, table_id, "edge_has_file_table")
           
            column_names = [field.name for field in table.schema]
            column_types = [field.type for field in table.schema]
            
            for name, dtype in zip(column_names, column_types):
                column_id = self.create_nodes(name, "column", parameters={"column_name": name, "col_type": str(dtype), "max_col_length": random.randint(20, 200)})
                self.create_edges(table_id, column_id, "edge_has_table_col")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    def delete_all_nodes(self):
        with self.driver.session() as session:
            session.execute_write(self._delete_all)

    @staticmethod
    def _delete_all(tx):
        tx.run("MATCH (n) DETACH DELETE n")
                
uri = "neo4j://localhost:7687"
user = "neo4j"
password = "12345678"

graph = Neo4jManager(uri, user, password)
graph.traverse_directory("./data/adventureworks/csv/")
# graph.delete_all_nodes()
graph.close()