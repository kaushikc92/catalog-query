import numpy as np
import torch
import os
import json
from sklearn.metrics.pairwise import cosine_similarity

SQL_SCHEMA_PATH = os.getenv('SQL_SCHEMA_PATH')
print(SQL_SCHEMA_PATH)

def get_roberta_embeddings(sentences, model, tokenizer):
    embeddings = []
    for sentence in sentences:
        inputs = tokenizer(sentence, return_tensors='pt', truncation=True,
            padding=True, max_length=512)
        with torch.no_grad():
            outputs = model(**inputs)
        sentence_embedding = outputs.last_hidden_state.mean(dim=1).squeeze().numpy()
        embeddings.append(sentence_embedding)
    return np.array(embeddings)

def get_top_relevant_tables(query_embedding, table_description_embeddings, table_names, top_n=5):
    similarities = cosine_similarity([query_embedding], table_description_embeddings)[0]
    top_indices = np.argsort(similarities)[-top_n:][::-1]
    return [table_names[i] for i in top_indices]

def add_schema_from_embeddings(queries, model, tokenizer):
    print(SQL_SCHEMA_PATH)
    sql_schema = json.load(open(SQL_SCHEMA_PATH))
    natural_language_queries = [query['naturalLanguageQuery'] for query in queries]
    table_descriptions = []
    table_names = []
    for table in sql_schema:
        columns = sql_schema[table]
        table_description = table + " " + " ".join(columns)
        table_descriptions.append(table_description)
        table_names.append(table)

    query_embeddings = get_roberta_embeddings(natural_language_queries, model, tokenizer)
    table_description_embeddings = get_roberta_embeddings(table_descriptions, model, tokenizer)

    for query, query_embedding in zip(queries, query_embeddings):
        top_tables = get_top_relevant_tables(query_embedding,
            table_description_embeddings, table_names)
        query['tablesFromEmbeddings'] = top_tables

    return queries
