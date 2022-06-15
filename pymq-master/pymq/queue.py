from abc import ABCMeta, abstractmethod


class QueueContract(metaclass=ABCMeta):
    @abstractmethod
    def uri(self) -> str:
        pass


class Queue(QueueContract):
    def __init__(self, uri: str):
        self.uri = uri

    def uri(self) -> str:
        return self.uri