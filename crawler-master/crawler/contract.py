from abc import ABC, abstractmethod
from urllib.parse import urlparse, ParseResult

from configs.crawler_settings import MIN_DOMAIN_LENGTH
from exceptions import WrongFormatError
from tools.structures import URLObject


class CrawlerContract(ABC):

    @abstractmethod
    def crawl(self, url_object: URLObject):
        pass


class AbstractCrawler(CrawlerContract):

    @staticmethod
    def create_url(host, schema=None):
        # if not host or type(host) != str or len(host) < MIN_DOMAIN_LENGTH:
        #     raise WrongFormatError("Wrong format for domain: {}".format(host))
        # parsed_result = urlparse(host)
        #
        # if parsed_result and parsed_result[0].lower() in ["http", "https"]:
        #     _schema = parsed_result[0].lower()
        # else:
        #     _schema = schema if schema is not None else "http"
        # url = ParseResult(_schema, *parsed_result[1:]).geturl()
        # return url
        # if not host or type(host) != str or len(host) < MIN_DOMAIN_LENGTH:
        #     raise WrongFormatError("Wrong format for domain: {}".format(host))
        # _schema = schema if schema is not None else "http"
        return f"https://{host}"

    @abstractmethod
    def crawl(self, url_object: URLObject):
        pass
