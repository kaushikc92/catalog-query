# Graph Database - neo4j
Note: Currently, all data has sucessfully been loaded into both graph and relational database. You can directly start the neo4j and postgresql server to check the data. The command are the follolwing. If you want to rebuilt the database, remember to drop all tables though the commands in the .py files. Then, you should be able to rerun the script to rebuilt all databases. 

Required directory:
1. **data** inside build-catalog [catalog-query/build-catalog] will store the raw data. The user can built a data catalog based on the raw data. 
2. **schema** in parent diretory [catalog-query] will store the short name and type_id of each table [nodes and edges]. The attributes for each table are also there. It will also store the taxonomy if the relational database will replicate the children's data into parents. 

## Intall necessary python module
```
pip3 install -r requirements.txt
```
## How to start neo4j database

```
./neo4j-community-5.20.0/bin/neo4j start
```
Then neo4j will build a web UI at http://localhost:7474/

## How to turn down neo4j database
```
./neo4j-community-5.20.0/bin/neo4j stop
```

## How to run the crawler to fetch the catalog
Remember to start the neo4j server at first.
```
python3 graphdb_crawler.py
```

# Relation Database - postgresql

## How to start the postgre server
```
./postgresql/bin/pg_ctl start -l ./postgresql/logfile -D ./postgresql/data
```

## How to turn down the postgre server
```
./postgresql/bin/pg_ctl stop -D ./postgresql/data
```

## Run the postgresql through terminal
```
#-U is the username, -d is the database name
./postgresql/bin/psql -U text2sql -d data_catalog
```

## Run the crawler to transfer from graphdb to relational db

Remember to start the neo4j and postgresql server first.
```
python3 relationaldb_crawler.py
```