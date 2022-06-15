import argparse
import logging.config
import os
import sys

import psycopg2
from dotenv import load_dotenv
from pymq import RabbitMQQueue, RabbitMQ, RabbitMQConnectionClient

sys.path.append(os.path.abspath(os.curdir))

from repository.repository import RepositoryContract
from tools.structures import CrawlingType

from configs.logging_config import logging_config
from queues.output_queue import OutputQueue, RMQQueue
from repository.domain_repository import FakeDomainRepository, DBDomainRepository
from service_locator import service_locator


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--repository",
                        choices=["db", "fake"],
                        default="db",
                        help="Domains repository: fake or db")
    parser.add_argument("-ct", "--crawling_type",
                        choices=["source", "render", "spf", "mx", "cname", "ns", "soa", "txt", "http"],
                        default="render", help="Type of crawling")
    parser.add_argument("-l", "--limit", type=int, default=100,
                        help="Limit of records. Default is 10. Works with db only")
    return parser.parse_args()


def populate(crawling_type, limit):
    domain_repository_ = service_locator.get(RepositoryContract)
    output_queue_ = service_locator.get(OutputQueue)
    with open("/home/proplus/Documents/Domains.csv") as file:
    csv_reader=csv.reader.read(file)
    for entity in csv_reader.readline(crawling_type, limit):
    #for entity in domain_repository_.all(crawling_type, limit):
        output_message = {
            "domain": entity.domain,
            "crawling_type": entity.crawling_type.value
        }
        output_queue_.put(output_message)


def repository_map(key):
    if key == "db":
        pg = psycopg2.connect(os.getenv("POSTGRESQL_CONNECTION_STRING"))
        repository = DBDomainRepository(pg)
    elif key == "fake":
        repository = FakeDomainRepository()
    else:
        raise RuntimeError("Key {} is not mapped to any repository".format(key))

    return repository


if __name__ == '__main__':
    load_dotenv()
    logging.config.dictConfig(logging_config)
    args = parse_args()

    crawling_type = CrawlingType.get_by_index(args.crawling_type)
    domain_repository = repository_map(args.repository)
    service_locator.bind(RepositoryContract, domain_repository)

    domains_queue = RabbitMQQueue(uri=os.getenv("DOMAINS_QUEUE_NAME"),
                                  exchange=os.getenv("DOMAINS_EXCHANGE_NAME"),
                                  routing=os.getenv("DOMAINS_ROUTING_KEY"))

    domains_rmq = RabbitMQ(domains_queue, RabbitMQConnectionClient(os.getenv("RABBITMQ_CONNECTION_STRING")))
    domains_rmq.connect()
    service_locator.bind(OutputQueue, RMQQueue(domains_rmq))

    populate(crawling_type, args.limit)
