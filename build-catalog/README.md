# Graph Database - neo4j
## Intall necessary module

```
pip3 install -r requirements.txt
```
## How to start neo4j database

```
./neo4j-community-5.20.0/bin/neo4j start
```
## How to run the scrawler to fetch the catalog
```
python3 graphdb_crawler.py
```

# Relation Database - postgresql

## How to start the server
```
./postgresql/bin/pg_ctl start -l ./postgresql/logfile -D ./postgresql/data
```

## How to turn down the server
```
./postgresql/bin/pg_ctl stop -D ./postgresql/data
```

## Run the postgresql through terminal
```
#-U is the username, -d is the database name
./postgresql/bin/psql -U text2sql -d data_catalog
```