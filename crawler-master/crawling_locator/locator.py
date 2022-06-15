from configs.crawler_settings import MIN_DOMAIN_LENGTH
from crawler.contract import CrawlerContract, AbstractCrawler
from exceptions import WrongFormatError
from tools.structures import CrawlingType, URLObject


class CrawlingEngineLocator(AbstractCrawler):

    def __init__(self):
        self._engines = {}

    @staticmethod
    def create_url(host, schema=None):
        # if not host or type(host) != str or len(host) < MIN_DOMAIN_LENGTH:
        #     raise WrongFormatError("Wrong format for domain: {}".format(host))
        # _schema = schema if schema is not None else "http"
        return "{}://{}".format("https", host)

    def add(self, _type: CrawlingType, crawler_inst: CrawlerContract):
        if not isinstance(_type, CrawlingType):
            raise RuntimeError("Type has wrong format. Should be CrawlingType enum")

        self._engines[_type] = crawler_inst

    def locate(self, _type: CrawlingType):
        if _type not in self._engines:
            raise RuntimeError("The type can't be located")

        return self._engines[_type]

    def crawl(self, url_object: URLObject):
        crawler = self.locate(url_object.crawling_type)
        return crawler.crawl(url_object)
