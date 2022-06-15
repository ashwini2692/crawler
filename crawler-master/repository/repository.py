from abc import ABC, abstractmethod


class RepositoryContract(ABC):

    @abstractmethod
    def all(self, *args, **kwargs):
        pass
