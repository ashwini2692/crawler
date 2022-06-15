#!/usr/bin/env python3
import argparse

from elasticsearch import Elasticsearch

DEFAULT_NUMBER_OF_SHARDS = 9
DEFAULT_NUMBER_OF_REPLICAS = 1


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True, help="Index name")
    # parser.add_argument("--endpoint", required=True)
    parser.add_argument("--number-of-shards", type=int, default=DEFAULT_NUMBER_OF_SHARDS)
    parser.add_argument("--number-of-replicas", type=int, default=DEFAULT_NUMBER_OF_REPLICAS)
    return parser.parse_args()


def main():
    args = parse_args()

    es = Elasticsearch("https://search-tecgence-elastic-a4qsep3qxcteliksocr5o3qxly.ap-northeast-1.es.amazonaws.com")
    result = es.indices.create(args.name, body={
        "settings": {
            "number_of_shards": args.number_of_shards,
            "number_of_replicas": args.number_of_replicas,
        },
        "mappings": {
            "properties": {
                "body": {
                    "type": "text",
                },
                "domain": {
                    "type": "text",
                },
                "page_url": {
                    "type": "text",
                },
                "timestamp": {
                    "type": "date",
                },
            }
        }
    })
    print(result)


if __name__ == '__main__':
    main()
