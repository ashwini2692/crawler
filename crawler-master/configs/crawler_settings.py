import os

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
}

MIN_DOMAIN_LENGTH = 3
CRAWLER_WORKER_NUM = 5
DEFAULT_BATCH_SIZE = os.getenv("DEFAULT_BATCH_SIZE",1000)
