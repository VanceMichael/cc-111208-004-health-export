import json
from urllib.parse import urlsplit


class ManifestError(ValueError):
    pass


def _file_item(kind, item, index):
    if not isinstance(item, dict):
        raise ManifestError('Invalid %s file item %d' % (kind, index))
    for name in ('type', 'url'):
        value = item.get(name)
        if not isinstance(value, str) or not value:
            raise ManifestError('Invalid %s file item %d: %s must be a non-empty string'
                                % (kind, index, name))
    parsed = urlsplit(item['url'])
    if parsed.scheme not in ('http', 'https') or not parsed.hostname:
        raise ManifestError('Invalid %s file item %d: url must be an http(s) target'
                            % (kind, index))
    if parsed.username is not None or parsed.password is not None:
        raise ManifestError('Invalid %s file item %d: credentials must not appear in urls'
                            % (kind, index))
    return {'kind': kind, 'type': item['type'], 'url': item['url']}


def parse_manifest(payload):
    """Parse and validate a completed bulk-export manifest.

    Returns a plain structure; never dereferences urls.
    """
    try:
        document = json.loads(payload)
    except (TypeError, ValueError) as error:
        raise ManifestError('Manifest is not valid JSON') from error
    if not isinstance(document, dict):
        raise ManifestError('Manifest must be a JSON object')
    if not isinstance(document.get('transactionTime'), str) or not document['transactionTime']:
        raise ManifestError('Missing transactionTime')
    if type(document.get('requiresAccessToken')) is not bool:
        raise ManifestError('requiresAccessToken must be boolean')
    output = document.get('output')
    if not isinstance(output, list):
        raise ManifestError('output must be an array')
    error = document.get('error', [])
    if not isinstance(error, list):
        raise ManifestError('error must be an array')
    output_items = [_file_item('output', item, index) for index, item in enumerate(output)]
    error_items = [_file_item('error', item, index) for index, item in enumerate(error)]
    return {
        'transactionTime': document['transactionTime'],
        'requiresAccessToken': document['requiresAccessToken'],
        'output': output_items,
        'error': error_items,
    }
