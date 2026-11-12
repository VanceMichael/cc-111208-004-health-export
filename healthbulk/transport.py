import time
from contextlib import contextmanager
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


REDIRECT_STATUSES = frozenset((301, 302, 303, 307, 308))


class TargetNotTrusted(ValueError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, target):
        return None


def origin_of(url):
    """Return a normalized scheme://host:port origin, rejecting credentials."""
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise ValueError('Invalid URL') from error
    if parsed.scheme not in ('http', 'https'):
        raise ValueError('Only HTTP(S) targets are accepted')
    if not parsed.hostname:
        raise ValueError('URL has no host')
    if parsed.username is not None or parsed.password is not None:
        raise ValueError('Credentials in URLs are not accepted')
    if port is None:
        port = 443 if parsed.scheme == 'https' else 80
    return '%s://%s:%d' % (parsed.scheme, parsed.hostname.lower(), port)


def normalize_trusted(origins):
    return tuple(sorted({origin_of(origin) for origin in origins}))


def require_trusted(url, trusted):
    """Validate a URL against a configured target set before any I/O."""
    origin = origin_of(url)
    if trusted is not None and origin not in set(trusted):
        raise TargetNotTrusted('Target origin is not configured as trusted: ' + origin)
    return origin


def retry_after_seconds(value, now=None):
    """Parse Retry-After as integer seconds or an HTTP date; None if absent."""
    if value is None:
        return None
    text = value.strip()
    if text.isdigit():
        return int(text)
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError) as error:
        raise ValueError('Invalid Retry-After value') from error
    if moment is None:
        raise ValueError('Invalid Retry-After value')
    if moment.tzinfo is None:
        # RFC 7231 dates without an explicit zone are GMT.
        seconds = moment.replace(tzinfo=timezone.utc).timestamp()
    else:
        seconds = moment.timestamp()
    clock = time.time() if now is None else now
    return max(0.0, seconds - clock)


def _once(url, method, data, headers, timeout):
    request = Request(url, data=data, method=method, headers=dict(headers or {}))
    opener = build_opener(ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=timeout)
    except HTTPError as error:
        response = error
    return response


def _strip_fields(headers, names):
    removed = {name.lower() for name in names}
    return {key: value for key, value in headers.items() if key.lower() not in removed}


class FetchResult:
    def __init__(self, status, headers, body, url, history):
        self.status = status
        self.headers = headers
        self.body = body
        self.url = url
        self.history = history


def fetch(url, method='GET', data=None, headers=None, trusted=None,
          timeout=5, maximum=4194304, max_redirects=5, observer=None):
    """Fetch with explicit redirect handling and credential scoping.

    Authorization is retained only for same-origin redirects. Every hop must
    belong to the configured trusted target set.
    """
    require_trusted(url, trusted)
    current = url
    current_headers = dict(headers or {})
    history = []
    for _ in range(max_redirects + 1):
        response = _once(current, method, data, current_headers, timeout)
        try:
            body = response.read(maximum + 1)
        finally:
            response.close()
        status = response.status
        response_headers = dict(response.headers.items())
        _observe(observer, current, method, status, current_headers)
        if status in REDIRECT_STATUSES and response_headers.get('Location'):
            target = urljoin(current, response_headers['Location'])
            require_trusted(target, trusted)
            history.append({'status': status, 'from': current, 'to': target})
            if origin_of(target) != origin_of(current):
                current_headers = _strip_fields(current_headers, ('authorization', 'cookie'))
            current = target
            continue
        if len(body) > maximum:
            raise ValueError('Response exceeds byte limit')
        return FetchResult(status, response_headers, body, current, history)
    raise ValueError('Too many redirects')


def _observe(observer, url, method, status, headers):
    if observer is None:
        return
    observer({'url': url, 'method': method, 'status': status,
              'authorization_sent': any(name.lower() == 'authorization' for name in headers)})


@contextmanager
def open_stream(url, headers=None, trusted=None, timeout=5, max_redirects=5, observer=None):
    """Open a GET response for incremental reading, applying the same
    trusted-target and redirect credential rules as fetch()."""
    require_trusted(url, trusted)
    current = url
    current_headers = dict(headers or {})
    history = []
    for _ in range(max_redirects + 1):
        response = _once(current, 'GET', None, current_headers, timeout)
        status = response.status
        response_headers = dict(response.headers.items())
        _observe(observer, current, 'GET', status, current_headers)
        if status in REDIRECT_STATUSES and response_headers.get('Location'):
            target = urljoin(current, response_headers['Location'])
            response.close()
            require_trusted(target, trusted)
            history.append({'status': status, 'from': current, 'to': target})
            if origin_of(target) != origin_of(current):
                current_headers = _strip_fields(current_headers, ('authorization', 'cookie'))
            current = target
            continue
        try:
            yield FetchResult(status, response_headers, None, current, history), response
        finally:
            response.close()
        return
    raise ValueError('Too many redirects')


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
