import json


def inspect(payload):
    document = json.loads(payload)
    if not isinstance(document.get('transactionTime'), str):
        raise ValueError('Missing transactionTime')
    if type(document.get('requiresAccessToken')) is not bool:
        raise ValueError('requiresAccessToken must be boolean')
    files = []
    for kind in ('output', 'error'):
        for item in document.get(kind, []):
            if not isinstance(item.get('type'), str) or not isinstance(item.get('url'), str):
                raise ValueError('Invalid manifest file item')
            files.append({'kind': kind, 'type': item['type'], 'url': item['url']})
    return {'transactionTime': document['transactionTime'],
            'requiresAccessToken': document['requiresAccessToken'], 'files': files}
