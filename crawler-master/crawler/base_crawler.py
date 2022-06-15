from dataclasses import dataclass
from typing import Optional

from crawler.contract import AbstractCrawler
from engine.contract import EngineContract
from tools.structures import URLObject


@dataclass
class Response:
    text: Optional[str] = None


class BaseCrawler(AbstractCrawler):
    def __init__(self, engine: EngineContract):
        self._engine = engine

    def crawl(self, url: URLObject):
        response = self._engine.request(url.url, crawling_type=None)
        return response
