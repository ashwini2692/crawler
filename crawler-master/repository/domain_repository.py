import logging
from typing import NamedTuple, Iterable

from psycopg2.extras import DictCursor

from repository.repository import RepositoryContract
from tools.structures import CrawlingType

logger = logging.getLogger(__name__)


class DomainEntity(NamedTuple):
    domain: str
    crawling_type: CrawlingType


class DBDomainRepository(RepositoryContract):
    def __init__(self, db):
        self.__db = db

    def all(self, crawling_type, limit=10) -> Iterable[DomainEntity]:
        with self.__db.cursor(cursor_factory=DictCursor) as cursor:
            try:
                cursor.execute('SELECT domain FROM domains LIMIT {}'.format(limit))
            except Exception:
                logger.error("Fail to get all domains")
                return []
            for row in cursor:
                yield DomainEntity(domain=row["domain"], crawling_type=crawling_type)


class FakeDomainRepository(RepositoryContract):
    def all(self, crawling_type, limit=None) -> Iterable[DomainEntity]:
        for domain, _type in [("stores.jtb.co.jp", crawling_type),
                              ("kuronekoyamato.co.jp", crawling_type),
                              ("yoshinoya.com", crawling_type),
                              ("e-welcia.com", crawling_type),
                              ]:
            yield DomainEntity(domain=domain, crawling_type=_type)
