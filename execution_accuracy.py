import psycopg
import subprocess
from psycopg.rows import dict_row
import json
from post_processor import SQLValidator
import pandas as pd
from datetime import datetime

def start_postgresql(data_dir):
    try:
        # Check the status of PostgreSQL
        status_result = subprocess.run(['./build-catalog/postgresql/bin/pg_ctl', 'status', '-D', data_dir], text=True, capture_output=True, check=False)
        # If PostgreSQL is not running, the status command will not succeed
        if "is running" not in status_result.stdout:
            print("PostgreSQL is not running. Attempting to start...")
            subprocess.run(['./build-catalog/postgresql/bin/pg_ctl', 'start', '-l', './build-catalog/postgresql/logfile', '-D', data_dir], check=True)
        else:
            print("PostgreSQL is already running.")     
    except subprocess.CalledProcessError as e:
        print(f"Failed to start PostgreSQL server: {e}")


def stop_postgresql(data_dir):
    try:
        subprocess.run(['./build-catalog/postgresql/bin/pg_ctl', 'stop', '-D', data_dir], check=True)
    except subprocess.CalledProcessError as e:
        print("Failed to stop PostgreSQL server:", e)

def normalize_results(rows):
    # column reorder
    # normalized_rows = [dict(sorted(row.items())) for row in rows]

    # row reorder
    sort_keys = sorted(rows[0].keys())
    # Sort the rows based on all keys
    normalized_rows = sorted(
        rows,
        key=lambda x: tuple(x[key] for key in sort_keys)
    )
    return normalized_rows

def compare_sql_outputs(gold_sql, predicted_sql, ignore_row_order=True):
    """
    Compares the outputs of two SQL queries, with separate error handling for each query.

    Parameters:
    gold_sql (str): SQL query to retrieve the gold standard results.
    predicted_sql (str): SQL query to retrieve the predicted results.
    ignore_row_order (bool): If True, the order of rows in the results will be ignored when comparing.

    Returns:
    bool: True if both queries produce the same normalized results, otherwise False. If the gold query fails, None is returned.
    """
    # Database connection parameters
    postgreq_params = {
        "host": "localhost",
        "dbname": "data_catalog",
        "user": "text2sql"
    }

    with psycopg.connect(**postgreq_params) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            # Process the gold standard query
            try:
                cursor.execute(gold_sql)
                gold_results = cursor.fetchall()
                print("Gold results:", gold_results)
                if gold_results and ignore_row_order:
                    normalized_gold = normalize_results(gold_results)
                else: 
                    normalized_gold = gold_results

            except psycopg.Error as e:
                print("Gold query can not be runned.\nError:", e)
                return None, None, None

            # Process the predicted query
            try:
                cursor.execute(predicted_sql)
                predicted_results = cursor.fetchall()
                print("Predicted results:", predicted_results)

                if predicted_results and ignore_row_order:
                    normalized_predicted = normalize_results(predicted_results)
                else: 
                    normalized_predicted = predicted_results
            except psycopg.Error as e:
                print("Predicted query can not be runned.\nError:", e)
                return False, normalized_gold, None
            # Compare normalized results
            return normalized_gold == normalized_predicted, normalized_gold, normalized_predicted

if __name__ == "__main__":
    
    # PostgreSQL data directory
    # data_dir = "./build-catalog/postgresql/data/"
    # start_postgresql(data_dir)

    RESULTS_PATH = 'results.json'
    results = json.load(open(RESULTS_PATH))
    n_previous = len(results)
    n = n_previous
    invalid_gold = []
    p = 0
    r = 0
    empty_output = 0
    validator = SQLValidator(schema_match=True)
    for res in results:
        print(validator.validate_query(res['predictedQuery']))
        if validator.validate_query(res['predictedQuery']):
            res['predict?'] = 1
            r += 1
            execution_correct, normalized_gold, normalized_predicted = compare_sql_outputs(res['goldSqlQuery'], res['predictedQuery'], ignore_row_order=True)
            if execution_correct:
                res['match?'] = 1
                if not normalized_predicted: # check empty list
                    res['empty?'] = 1
                    print("Predicted results are empty.")
                    empty_output += 1
                else:
                    res['empty?'] = 0
                p += 1
            else:
                res['match?'] = 0
                res['empty?'] = -1
            if execution_correct is None:
                n -= 1
                invalid_gold.append(res['goldSqlQuery'])
        else:
            res['predict?'] = 0
            res['match?'] = -1
            res['empty?'] = -1
                
    print(f'Total Samples: {n_previous}')  
    print(f'Valid Samples: {n}')
    print(f'Precision: {p}/{r} = {p/r}')
    print(f'Empty Predicted Result in Precision: {empty_output}/{p} = {empty_output/p}')
    print(f'Recall: {p}/{n} = {p/n}')
    if invalid_gold:
        json.dump(invalid_gold, open('invalid_gold.json', 'w'), indent=2)
    filename = 'run-s0-{}-v1.csv'.format(datetime.now().strftime("%b%d-%Y"))
    pd.DataFrame(results).to_csv(filename, index=False)
    # stop_postgresql(data_dir)
