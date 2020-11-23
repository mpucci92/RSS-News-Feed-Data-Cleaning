from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd

# Setting up the Elastic Search Connection #  # EPHERMAL PORTS USED EACH TIME VMs ARE CLOSED
es_client = Elasticsearch(['34.95.46.239:8607'],http_compress=True)

# Structure for the Index to be used for ElasticSearch #

settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.mapping.ignore_malformed": True
    },
    "mappings": {
        "properties": {
                    "title": {
                        "type": "text"
                    },
                    "link": {
                        "type": "text"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "YYYY-MM-DD||YYYY-MM-DD HH:mm:ss||YYYY-MM-DD HH:mm:ssZ||YYYY-MM-DD HH:mm:ss.n||strict_date_hour_minute_second"
                    },
                    "oscrap_timestamp": {
                        "type": "date",
                        "format": "YYYY-MM-DD||YYYY-MM-DD HH:mm:ss||YYYY-MM-DD HH:mm:ssZ||YYYY-MM-DD HH:mm:ss.n||YYYY-DD-MM HH:mm:ss"
                    },
                    "authors": {
                        "type": "keyword"
                    },
                    "article_type": {
                        "type": "keyword"
                    },
                    "tickers": {
                        "type": "keyword"
                    },
                    "summary": {
                        "type":"text"
                    },
                    "_summary": {
                        "type":"text"
                    },
                    "language": {
                        "type":"keyword"
                    },
                    "categories": {
                        "type":"keyword"
                    },
                    "related": {
                        "type":"keyword"
                    },
                    "_tickers": {
                        "type":"keyword"
                    },
                    "credit": {
                        "type":"keyword"
                    },
                    "sentiment":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}
                    },
                    "sentiment_score":{"type":"float","ignore_malformed": True}
                }
            }
        }

# Create an Index
es_client.indices.create(index='news',body=settings)