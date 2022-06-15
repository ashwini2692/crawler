import hashlib
import json
import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime

from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)


class OutputQueue(metaclass=ABCMeta):
    @abstractmethod
    def put(self, message: str):
        pass


class OutputQueueComposite(OutputQueue):
    def __init__(self):
        self._queues = []

    def add(self, queue):
        self._queues.append(queue)

    def put(self, message: str):
        for queue in self._queues:
            queue.put(message)


class PrintingQueue(OutputQueue):
    def put(self, message: str):
        print(message)


class RMQQueue(OutputQueue):
    def __init__(self, rmq):
        self.rmq = rmq

    def put(self, message_json: dict):
        try:
            self.rmq.send(json.dumps(message_json))
        except Exception as e:
            logger.error("Can't send message to a queues: ", str(e),
                         exc_info=True)


class ESIndexQueue(OutputQueue):
    def __init__(self, elastic_search, index, batch_size=1):
        self._elastic_search = elastic_search
        self._batch_size = batch_size
        self._index = index
        self.__batch = []

    @staticmethod
    def crawling_types_mapping():
        return {
            "source": "body",
            "render": "body",
            "spf": "spf",
            "mx": "mx",
            "cname": "cname",
            "ns": "ns",
            "soa": "soa",
            "txt": "txt",
            "careers": "careers_page",
        }

    def map_crawling_type_field(self, document):
        _mapping = self.crawling_types_mapping()
        crawling_type = document.get("crawling_type")
        if crawling_type is None:
            raise RuntimeError("Empty output crawling type")

        mapped_type = _mapping.get(crawling_type)
        if mapped_type is None:
            raise RuntimeError("Mapping type is missed")

        return mapped_type

    def create_document_to_index(self, document):
        mapped_field = self.map_crawling_type_field(document)
        doc_to_index = {
            "doc": {
                mapped_field: document["response"],
                "domain": document["domain"],
                "timestamp": datetime.utcnow()
            },
            "doc_as_upsert": True
        }
        if document["page_url"]:
            doc_to_index["doc"]["page_url"] = document["page_url"]
        if document["headers"]:
            doc_to_index["doc"]["headers"] = document["headers"]
        if document["web_requests"]:
            doc_to_index["doc"]["web_requests"] = document["web_requests"]

        return doc_to_index

    @staticmethod
    def _is_indexed(response):
        if "errors" in response and "items" in response \
                and response["errors"]:
            errors_string = ""
            for item in response["items"]:
                item_error = item.get("index", {}).get("error", "")
                errors_string += item_error + "; " if item_error else ""
            logger.error("Fail to index: ", errors_string)
            return False
        return True

    def process_es_bulk(self):
        try:
            bulk_response = bulk(self._elastic_search, self.__batch,
                                 raise_on_error=False)
        except Exception as e:
            logger.error("Fail to bulk process: {}".format(e))
            return
        if self._is_indexed(bulk_response):
            logger.info("Bulk is inserted")
            self.__batch = []
            return
        else:
            logger.error(
                "Domains data indexed with errors: {}".format(bulk_response),
                exc_info=True)

    def put(self, doc: dict):
        try:
            document_to_index = self.create_document_to_index(doc)
            domain = str(doc["domain"])
            if domain.endswith('/'):
                domain = domain.strip('/')
            _ = self._elastic_search.update(
                index=self._index,
                id=hashlib.md5(domain.encode("utf-8")).hexdigest(),
                body=document_to_index)
        except Exception as e:
            logger.error(f"Fail to index document: {e}", exc_info=True)
            return
