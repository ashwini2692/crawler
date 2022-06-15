import json
import logging

import requests

from engine.contract import EngineContract


logger = logging.getLogger(__name__)


class RenderAPIEngine(EngineContract):

    def __init__(self, render_api_url, headers):
        self._render_api_url = render_api_url
        self.headers = headers if headers is not None else {}

    def _get_response(self, _schema, domain):
        url = f"{_schema}://{domain}"
        request_params = {
            "url": url,
            "headers": self.headers,
            "html": 1,
            "timeout": 300,
            "images": 0,
            "wait": 1.5,
            "har": 1,
            "history": 1,
        }
        try:
            response = requests.post(
                self._render_api_url,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(request_params)
            )
            response_status = response.status_code
            if response_status < 200 or response_status >= 400:
                logger.warning(f"Response {response_status} from {url}")
                return None
            if response_status == 204:
                logger.warning(f"Response {response_status} from {url}")
                return None
            return response
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSLError occurred for {url}: {e}")
            return None
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f"Read timeout occurred. for {url}: {e}")
            return None
        except requests.exceptions.ConnectTimeout as e:
            logger.warning(f"Connection timeout occurred for {url}: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"ConnectionError occurred for {url}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTPError occurred for url {url}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Exception for url: {url}. {e}")
            return None

    def request(self, domain, crawling_type=None):
        _schema = "https"
        resp = self._get_response(_schema, domain)
        if not resp:
            _schema = "https"
            resp = self._get_response(_schema, "www." + domain)
            if not resp:
                _schema = "http"
                resp = self._get_response(_schema, domain)
                if not resp:
                    _schema = "http"
                    resp = self._get_response(_schema, "www." + domain)
        return resp
