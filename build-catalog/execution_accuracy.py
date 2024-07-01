import psycopg
import subprocess
from psycopg.rows import dict_row

def start_postgresql(data_dir):
    try:
        # Check the status of PostgreSQL
        status_result = subprocess.run(['./postgresql/bin/pg_ctl', 'status', '-D', data_dir], text=True, capture_output=True, check=False)
        # If PostgreSQL is not running, the status command will not succeed
        if "is running" not in status_result.stdout:
            print("PostgreSQL is not running. Attempting to start...")
            subprocess.run(['./postgresql/bin/pg_ctl', 'start', '-l', './postgresql/logfile', '-D', data_dir], check=True)
        else:
            print("PostgreSQL is already running.")     
    except subprocess.CalledProcessError as e:
        print(f"Failed to start PostgreSQL server: {e}")


def stop_postgresql(data_dir):
    try:
        subprocess.run(['./postgresql/bin/pg_ctl', 'stop', '-D', data_dir], check=True)
    except subprocess.CalledProcessError as e:
        print("Failed to stop PostgreSQL server:", e)

def normalize_results(rows, sort_keys):
    # column reorder
    # normalized_rows = [dict(sorted(row.items())) for row in rows]

    # Rows reorder
    if sort_keys:
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
                if gold_results and "node_id" in gold_results[0].keys():
                    sort_keys = ["node_id"]
                else:
                    sort_keys = ["edge_id"]
                if ignore_row_order:
                    normalized_gold = normalize_results(gold_results, sort_keys)
                else: 
                    normalized_gold = gold_results

            except psycopg.Error as e:
                print("Gold query can not be runned.\nError:", e)
                return None

            # Process the predicted query
            try:
                cursor.execute(predicted_sql)
                predicted_results = cursor.fetchall()
                print("Predicted results:", predicted_results)

                if predicted_results and "node_id" in predicted_results[0].keys():
                    sort_keys = ["node_id"]
                else:
                    sort_keys = ["edge_id"]
                if ignore_row_order:
                    normalized_predicted = normalize_results(predicted_results, sort_keys)
                else: 
                    normalized_predicted = predicted_results
            except psycopg.Error as e:
                print("Predicted query can not be runned.\nError:", e)
                return False
            # Compare normalized results
            return normalized_gold == normalized_predicted

if __name__ == "__main__":
    
    # PostgreSQL data directory
    data_dir = "./postgresql/data/"
    start_postgresql(data_dir)

    # Compare SQL outputs
    gold_sql = "SELECT short_name, node_id FROM node_directory ORDER BY short_name;"
    predicted_sql = "SELECT node_id, short_name FROM node_directory;"
    result = compare_sql_outputs(gold_sql, predicted_sql, ignore_row_order=True)
    print("Same output?", result)

    stop_postgresql(data_dir)
