class CrawlError(Exception):
    pass


class EngineError(Exception):
    pass


class RequestError(EngineError):
    pass


class WrongFormatError(Exception):
    pass


class WrongCrawlingType(Exception):
    pass
