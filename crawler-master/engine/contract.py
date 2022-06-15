from abc import ABC, abstractmethod


class EngineContract(ABC):
    @abstractmethod
    def request(self, url, crawling_type):
        pass
