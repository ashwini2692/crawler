from abc import ABCMeta, abstractmethod
from typing import List

from .queue import QueueContract


class MessageQueueContract(metaclass=ABCMeta):

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def receive(self) -> List[str]:
        pass

    @abstractmethod
    def queue(self) -> QueueContract:
        pass