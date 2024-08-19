import json, re, csv, argparse, os
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
from rag.schema_selector.embeddings import add_schema_from_embeddings
from rag.schema_selector.tfidf import add_schema_from_tfidf
from utils.camelcase_tokenizer import CamelCaseTokenizer
from transformers import RobertaTokenizer, RobertaModel

def generate_attributes(gold_sql_query, schema):
    tables = []
    tokens = gold_sql_query.split(' ')
    for token in tokens:
        if token in schema:
            tables.append(token)
    return {
        'isAggregate': len(re.findall(' group by ', gold_sql_query)) != 0,
        'isConditional': len(re.findall(' where ', gold_sql_query)) != 0 or len(re.findall(' having ', gold_sql_query)) != 0,
        'tables': tables
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate attributes for examples')
    parser.add_argument('--eg-path', type=str, default=os.getenv('EXAMPLES_PATH'))
    parser.add_argument('--eg-attrib-path', type=str, default=os.getenv('EXAMPLE_ATTRIBUTES_PATH'))
    parser.add_argument('--schema-path', type=str, default=os.getenv('SQL_SCHEMA_PATH'))
    args = parser.parse_args()
    
    csv_reader = csv.DictReader(open(args.eg_path, mode='r'))
    examples = list(map(lambda row: {'naturalLanguageQuery': row['naturalLanguageQuery'], 'goldSqlQuery': row['goldSqlQuery']}, csv_reader))
    schema = json.load(open(args.schema_path, 'r'))
    
    example_attributes = [{
        'eid': 'e' + str(i + 1), 
        **example, 
        **generate_attributes(example['goldSqlQuery'], schema)
    } for i, example in enumerate(examples)]

    tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
    model = RobertaModel.from_pretrained('roberta-base')
    example_attributes = add_schema_from_embeddings(
        example_attributes,
        model=model,
        tokenizer=tokenizer
    )

    tokenizer = CamelCaseTokenizer()
    example_attributes = add_schema_from_tfidf(example_attributes, tokenizer)

    json.dump(example_attributes, open(args.eg_attrib_path, 'w'), indent=2)
