import json
import logging
import time
from dataclasses import dataclass
from typing import NamedTuple, Union, Dict, List, Optional

from amqpstorm import Message
from pymq import MessageReceiveError, MessageQueueContract

from crawler.contract import AbstractCrawler
from exceptions import WrongCrawlingType, CrawlError
from queues.output_queue import OutputQueue
from tools.structures import URLObject, CrawlingType

logger = logging.getLogger(__name__)


@dataclass
class InputMessage:
    domain: str
    crawling_type: CrawlingType
    url_to_crawl: str


class OutputMessage(NamedTuple):
    domain: str
    crawling_type: str
    response: str
    page_url: Optional[str] = None
    headers: Optional[str] = None
    web_requests: Optional[str] = None


class CrawlingService:
    def __init__(self,
                 crawler: AbstractCrawler,
                 input_queue: MessageQueueContract,
                 output_queue: OutputQueue,
                 errors_queue: MessageQueueContract
                 ):
        self.crawler = crawler
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.errors_queue = errors_queue

    @staticmethod
    def load_message_into_json(message):
        if type(message) == Message:
            return json.loads(message.body)
        elif type(message) == dict:
            return message
        else:
            raise RuntimeError("Queue message wrong format")

    def input_message(self, message) -> InputMessage:
        message_json = self.load_message_into_json(message)

        if not message_json:
            logger.error("empty_domains_data", exc_info=True)
            raise RuntimeError("Empty message body")

        if "domain" not in message_json:
            logger.error("domain key is not found {}".format(message_json),
                         exc_info=True)
            raise RuntimeError("Domain key is not found")

        crawling_type = self.map_crawling_type(message_json)

        if crawling_type == "careers":
            url = message_json["url_to_crawl"]
        else:
            url = message_json["domain"]

        input_message = InputMessage(domain=message_json["domain"],
                                     crawling_type=crawling_type,
                                     url_to_crawl=url)
        return input_message

    def receive(self, message_batch_size: int):
        try:
            messages = self.input_queue.receive(message_batch_size,
                                                break_on_empty=True)
            return messages
        except MessageReceiveError as e:
            logger.error("fail_to_receive_messages".format(str(e)),
                         exc_info=True)
            return []

    def send(self, message: Union[Dict, List[Dict]]):
        if type(message) == list:
            for _m in message:
                self.output_queue.put(_m)
        else:
            self.output_queue.put(message)

    def ack_message(self, message):
        if hasattr(self.input_queue, "ack_message"):
            self.input_queue.ack_message(message)

    def create_crawling_url(self, message):
        return self.crawler.create_url(message["domain"])

    @staticmethod
    def map_crawling_type(message):
        crawling_type_str = message.get("crawling_type")
        if not crawling_type_str:
            logger.error("Empty input crawling type")
            raise WrongCrawlingType

        crawling_type = CrawlingType.get_by_index(crawling_type_str)
        if crawling_type is None:
            logger.error("Can't map crawling type")
            raise WrongCrawlingType
        return crawling_type

    def crawl_url(self, input_message: InputMessage):
        url_object = URLObject(url=input_message.url_to_crawl,
                               crawling_type=input_message.crawling_type)
        response = self.crawler.crawl(url_object)
        if response is None:
            logger.error(
                f"Empty response from {url_object.url}, {url_object.crawling_type}")
            return None
        return response

    @staticmethod
    def output_message(input_message, crawling_response):
        if input_message.crawling_type.value in ["render"]:
            crawling_response = crawling_response.json()
            headers = json.dumps(
                crawling_response
                .get("har", {})
                .get("log", {})
                .get("entries", [])[0]
                .get("response", {})
                .get("headers", {})
            )
            web_requests = []
            for web_request in crawling_response.get("har", {}).get("log", {}).get("entries", []):
                web_requests.append(web_request.get("request", {}).get("url", {}))
            web_requests = json.dumps(web_requests)
            page_url = crawling_response["requestedUrl"]
            response_txt = crawling_response["html"]
        else:
            headers = None
            web_requests = None
            page_url = None
            response_txt = crawling_response.text
        output_message = OutputMessage(
            domain=input_message.domain,
            response=response_txt,
            crawling_type=input_message.crawling_type.value,
            page_url=page_url,
            headers=headers,
            web_requests=web_requests,
        )
        output_message = output_message._asdict()
        return output_message

    def process_message(self, message):
        input_message: InputMessage = self.input_message(message)
        crawling_response = self.crawl_url(input_message)

        if crawling_response is None:
            raise CrawlError(
                f"For {input_message.domain} crawling_response is None")
        output_message = self.output_message(input_message, crawling_response)
        self.send([output_message])

    def process_single_batch(self, message_batch_size):
        messages = self.receive(message_batch_size)
        if messages is None:
            return
        messages_set = set(messages)
        for message in messages_set:
            try:
                self.process_message(message)
            except Exception as e:
                logger.error(f"Can't process message: {e}", exc_info=True)
                message_json = self.load_message_into_json(message)
                self.errors_queue.send(json.dumps(message_json))
            self.ack_message(message)

    def process(self, message_batch_size):
        while True:
            messages_iter = self.receive(message_batch_size)
            messages_set = set(messages_iter)
            for message in messages_set:
                try:
                    self.process_message(message)
                except Exception as e:
                    logger.error(f"Can't process message: {e}", exc_info=True)
                    message_json = self.load_message_into_json(message)
                    self.errors_queue.send(json.dumps(message_json))
                self.ack_message(message)

    def run(self, message_batch_size: int, single_batch):
        try:
            if single_batch:
                self.process_single_batch(message_batch_size)
            else:
                self.process(message_batch_size)
        except Exception as e:
            logger.error("fail_process_entity_global: {}".format(str(e)),
                         exc_info=True)
            time.sleep(5)
