import logging
from typing import List, Optional, Union
from urllib.parse import urlparse

from dns.rdatatype import UnknownRdatatype
from dns.resolver import Resolver, NoAnswer, Timeout

from crawler.base_crawler import BaseCrawler, Response
from engine.contract import EngineContract
from tools.structures import CrawlingType, URLObject

logger = logging.getLogger(__name__)


class DNSRecordsBaseEngine(EngineContract):

    @classmethod
    def search(cls, domain,
               record_type: CrawlingType,
               as_string=True,
               delimiter="\n") -> Optional[Union[List[str], str]]:
        resolver = Resolver()
        try:
            answers = resolver.resolve(domain, record_type.value)
        except NoAnswer as e:
            logger.error(f"No answer, {domain}, {record_type}, {e}")
            return
        except Timeout as e:
            logger.error(f"DNS Search Timeout, {domain}, {record_type}, {e}")
            return
        except UnknownRdatatype as e:
            logger.error(f"Unknown dns lookup type: {e}")
            return
        except Exception as e:
            logger.error(f"DNS lookup failed: {domain}, {record_type}, {e}")
            return
        _records = []
        for answer in answers:
            _records.append(answer.to_text().strip("'").strip('"').strip())
        if as_string:
            return delimiter.join(_records)
        return _records

    def request(self, url, crawling_type):
        url_object = urlparse(url)
        domain = url_object.hostname or url_object.path or url
        if domain.startswith("www."):
            domain = domain[4:]
        return self.search(domain, crawling_type)


class DNSRecordsCrawler(BaseCrawler):
    def crawl(self, url: URLObject):
        url_ = "https://" + url.url
        result = self._engine.request(url_, url.crawling_type)
        if result is None:
            return
        return Response(text=result)
