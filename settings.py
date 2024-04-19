SETTINGS = {
	"settings": {
    	"analysis": {
            "filter": {
                "my_ngram_filter": {
                    "type": "ngram",
                    "min_gram": 3,
                    "max_gram": 5
                },
                #"my_synonym_filter": {
                #    "type": "synonym",
                #    "synonyms_path": "synonyms.txt"
                #}
            },
            "analyzer": {
                "my_custom_analyzer": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    #"filter": ["lowercase", "my_synonym_filter", "my_ngram_filter"]
                    "filter": ["lowercase", "my_ngram_filter"]
                }
            }
        },
        "index": {
            "max_ngram_diff": 5 
        }
    },
    "mappings": {
        "dynamic_templates": [
            {
                "text_fields_use_customized_tokenizer": {
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "text",
                        "analyzer": "my_custom_analyzer"
                    }
                }
            }
        ]
    }
}
