import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

logger = logging.getLogger()
default_headers = {"Content-Type": "application/json"}
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)


def requests_retry_session(retries=10,
                           backoff_factor=0.3,
                           status_forcelist=(500, 502, 504),
                           session=None,
                           ):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class RestClient:
    def __init__(self):
        self.good_status = [200, 201, 202, 203, 204, 206, 207, 208, 226]
        self.redirection_status = [300, 301, 302, 303, 304, 305, 306, 307, 308]
        self.client_error_status = list(range(400, 451))
        self.internal_server_error = [500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511]

    def send_rest_request(self, service_url, method, auth=None, headers=None):
        if headers is None:
            headers = {"Accept": "application/json"}
        try:
            ret = requests_retry_session().get(url=service_url, headers=headers, auth=auth, timeout=30)

            if ret.status_code in self.good_status:

                return {
                    "response": ret.json(),
                    "status_code": ret.status_code
                }

            else:
                return 0
        except Exception as error:
            logger.error('EXCEPTION: %s sending %s request to %s ' % (error, method, service_url))
            return 0
