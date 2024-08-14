import csv
import json

# Read the CSV file
csv_file = 'queries.csv'
json_file = 'queries.json'

data = []
with open(csv_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append(row)

# Write to JSON file
with open(json_file, 'w') as f:
    json.dump(data, f, indent=4)
