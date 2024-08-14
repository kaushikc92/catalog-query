import pandas as pd
import json

# Read the CSV file
csv_file = 'queries.csv'
json_file = "queries.json"
df = pd.read_csv(csv_file)

# Rename columns
df.rename(columns={
    'example_id': 'eid',
    'nl_query': 'naturalLanguageQuery',
    'gold_query': 'goldSqlQuery',
    # Add more columns as needed
}, inplace=True)

# Convert to JSON
json_data = df.to_json(orient='records')

# Save to a file
with open(json_file, 'w') as f:
    json.dump(json.loads(json_data), f, indent = 4)
