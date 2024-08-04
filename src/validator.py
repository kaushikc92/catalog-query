import json
import psycopg
import os
import csv
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

EXAMPLES_PATH = os.getenv('EXAMPLES_PATH')
postgres_params = {
    "host": os.getenv('POSTGRES_HOST', 'localhost'),
    "dbname": os.getenv('POSTGRES_DB', 'postgres'),
    "user": os.getenv('POSTGRES_USER', 'postgres'),
    "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
    "port": int(os.getenv('POSTGRES_PORT', 5432))
}

conn = psycopg.connect(**postgres_params)
cursor =  conn.cursor()
f = open(EXAMPLES_PATH, mode='r')
csv_reader = csv.DictReader(f)
for row in csv_reader:
    q = row['gold_query']
    i = row['example_id']
    try:
        cursor.execute(q)
        gold_results = cursor.fetchall()
    except psycopg.Error as e:
        print("Id: {0},\nError: {1}\nQuery: {2}".format(i, e, q))
        break
