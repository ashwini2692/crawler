import argparse
import logging.config
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from pymq import RabbitMQQueue, RabbitMQ, RabbitMQConnectionClient

from configs.crawler_settings import DEFAULT_HEADERS, DEFAULT_BATCH_SIZE
from configs.logging_config import logging_config
from crawler.base_crawler import BaseCrawler
from crawler.crawling_service import CrawlingService
from crawling_locator.locator import CrawlingEngineLocator
from engine.dns_records import DNSRecordsCrawler, DNSRecordsBaseEngine
from engine.render_engine import RenderAPIEngine
from engine.requests_engine import RequestsEngine
from queues.data_queue import FakeDomainQueue
from queues.output_queue import ESIndexQueue, PrintingQueue
from tools.structures import CrawlingType

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input",
                        choices=["rmq", "fake"],
                        default="rmq",
                        help="Input queue: fake or rmq. RMQ by default")

    parser.add_argument("-o", "--output",
                        choices=["es", "console"],
                        default="es",
                        help="Output queue: console or ElasticSearch. ES by default")

    parser.add_argument("-e", "--error",
                        choices=["rmq", "console"],
                        default="rmq",
                        help="Errors queue: console or rmq. RMQ by default")

    parser.add_argument("-b", "--batch_size",
                        default=DEFAULT_BATCH_SIZE,
                        type=int,
                        help="Message batch size from an input queue")

    parser.add_argument("-sb", "--single_batch", help="Run in single batch mode?", action='store_true')

    return parser.parse_args()


def map_input_queue(key):
    if key == "rmq":
        domains_queue = RabbitMQQueue(uri=os.getenv("DOMAINS_QUEUE_NAME"),
                                      exchange=os.getenv("DOMAINS_EXCHANGE_NAME"),
                                      routing=os.getenv("DOMAINS_ROUTING_KEY"))
        queue = RabbitMQ(domains_queue, RabbitMQConnectionClient(os.getenv("RABBITMQ_CONNECTION_STRING")))
        queue.connect()
    elif key == "fake":
        queue = FakeDomainQueue()
    else:
        raise RuntimeError("Input queue can't be specified")
    return queue


def map_output_queue(key):
    if key == "es":
        es = Elasticsearch(os.getenv("ELASTICSEARCH_CONNECTION_STRING"))
        output = ESIndexQueue(es, os.getenv("ELASTICSEARCH_INDEX"))
    elif key == "console":
        output = PrintingQueue()
    else:
        raise RuntimeError("Output queue can't be specified")
    return output


def map_errors_queue(key):
    if key == "rmq":
        _errors_queue = RabbitMQQueue(uri=os.getenv("ERRORS_QUEUE_NAME"),
                                      exchange=os.getenv("ERRORS_EXCHANGE_NAME"),
                                      routing=os.getenv("ERRORS_ROUTING_KEY"))
        queue = RabbitMQ(_errors_queue, RabbitMQConnectionClient(os.getenv("RABBITMQ_CONNECTION_STRING")))
        queue.connect()
    elif key == "console":
        queue = PrintingQueue()
    else:
        raise RuntimeError("Errors queue can't be specified")
    return queue


if __name__ == '__main__':
    load_dotenv()
    logging.config.dictConfig(logging_config)
    args = parse_args()
    source_engine = RequestsEngine(headers=DEFAULT_HEADERS)
    render_engine = RenderAPIEngine(os.getenv("RENDER_API_URL"), headers=DEFAULT_HEADERS)
    crawling_locator = CrawlingEngineLocator()
    crawling_locator.add(CrawlingType.SOURCE, BaseCrawler(source_engine))
    crawling_locator.add(CrawlingType.CAREERS, BaseCrawler(source_engine))
    crawling_locator.add(CrawlingType.RENDER, BaseCrawler(render_engine))
    crawling_locator.add(CrawlingType.SPF, DNSRecordsCrawler(DNSRecordsBaseEngine()))
    crawling_locator.add(CrawlingType.CNAME, DNSRecordsCrawler(DNSRecordsBaseEngine()))
    crawling_locator.add(CrawlingType.MX, DNSRecordsCrawler(DNSRecordsBaseEngine()))
    crawling_locator.add(CrawlingType.NS, DNSRecordsCrawler(DNSRecordsBaseEngine()))
    crawling_locator.add(CrawlingType.SOA, DNSRecordsCrawler(DNSRecordsBaseEngine()))
    crawling_locator.add(CrawlingType.TXT, DNSRecordsCrawler(DNSRecordsBaseEngine()))

    input_queue = map_input_queue(args.input)
    output_queue = map_output_queue(args.output)
    errors_queue = map_errors_queue(args.error)

    crawling_service = CrawlingService(crawler=crawling_locator,
                                       input_queue=input_queue,
                                       output_queue=output_queue,
                                       errors_queue=errors_queue)
    crawling_service.run(message_batch_size=args.batch_size, single_batch=args.single_batch)
