from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, target):
        return None


def capture(url, method='GET', data=None, headers=None, maximum=4194304):
    parsed = urlsplit(url)
    if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('The inspection transport accepts loopback HTTP only')
    if parsed.username or parsed.password:
        raise ValueError('Credentials in URLs are not accepted')
    request = Request(url, data=data, method=method, headers=headers or {})
    opener = build_opener(ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=3)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(maximum + 1)
        if len(body) > maximum:
            raise ValueError('Inspection response exceeds byte limit')
        return response.status, dict(response.headers.items()), body
