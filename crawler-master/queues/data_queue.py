import itertools

from pymq import MessageQueueContract

from repository.domain_repository import FakeDomainRepository
from tools.structures import CrawlingType


class FakeDomainQueue(MessageQueueContract):

    def __init__(self):
        self.domain_repository = FakeDomainRepository()

    def queue(self):
        return

    def send(self, message):
        print(message)

    def receive(self, messages_number: int = 1):
        for entity in itertools.islice(self.domain_repository.all(CrawlingType.TXT), messages_number):
            yield {"domain": entity.domain, "crawling_type": entity.crawling_type.value}


class PrintingErrorQueue(MessageQueueContract):
    def queue(self):
        return

    def send(self, message):
        print(message)

    def receive(self):
        pass
