from dataclasses import dataclass
from enum import Enum


class CrawlingType(Enum):
    RENDER = "render"
    SOURCE = "source"
    SPF = "spf"
    CNAME = "cname"
    MX = "mx"
    NS = "ns"
    SOA = "soa"
    TXT = "txt"
    CAREERS = "careers"

    @classmethod
    def get_by_index(cls, index):
        for field in dir(cls):
            _attr = getattr(CrawlingType, field)
            if hasattr(_attr, "value") and getattr(_attr, "value") == index:
                return _attr
        raise RuntimeError("Type is not specified for {}".format(index))


@dataclass
class URLObject:
    url: str
    crawling_type: CrawlingType
