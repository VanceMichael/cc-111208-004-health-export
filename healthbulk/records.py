import json
import os
import re


class BadLine(ValueError):
    def __init__(self, line, message):
        super().__init__('Line %d: %s' % (line, message))
        self.line = line


REFERENCE = re.compile(r'^([A-Za-z][A-Za-z0-9]{0,63})/([^/\s]+)$')


def references_of(document):
    """Collect local relative references (Type/id) anywhere in a resource.

    Contained (#id), absolute urls and urn:uuid references are not resolved
    against this batch. Unresolvable references are retained as dangling.
    """
    found = set()

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == 'reference' and isinstance(child, str):
                    match = REFERENCE.match(child)
                    if match:
                        found.add((match.group(1), match.group(2)))
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(document)
    return sorted(found)


def read_ndjson(response, part_path, handle_line, checkpoint=None,
                expected_bytes=None, progress_path=None,
                max_bytes=67108864, max_line=1048576):
    """Stream newline-delimited JSON from a live response.

    Raw bytes are appended to part_path as they arrive, so a dropped
    connection leaves measurable progress on disk. Each non-empty line must
    be a UTF-8 JSON object; errors are reported with their 1-based line.
    A response shorter than its declared Content-Length is an error, never
    a silently accepted short file.
    """
    buffer = b''
    line_number = 0
    total = 0
    reader = getattr(response, 'read1', None) or response.read
    with open(part_path, 'wb') as sink:
        while True:
            if checkpoint is not None:
                checkpoint()
            chunk = reader(65536)
            if not chunk:
                break
            sink.write(chunk)
            sink.flush()
            total += len(chunk)
            if progress_path is not None:
                with open(progress_path, 'wb') as marker:
                    marker.write(str(total).encode())
            if total > max_bytes:
                raise BadLine(line_number + 1, 'File exceeds byte limit')
            buffer += chunk
            while b'\n' in buffer:
                raw, buffer = buffer.split(b'\n', 1)
                line_number += 1
                _consume(raw, line_number, handle_line, max_line)
        truncated = expected_bytes is not None and total != expected_bytes
        if buffer and not truncated:
            line_number += 1
            _consume(buffer, line_number, handle_line, max_line)
        try:
            sink.flush()
            os.fsync(sink.fileno())
        except OSError:
            pass
    if expected_bytes is not None and total != expected_bytes:
        raise BadLine(line_number, 'truncated response: received %d of %d bytes'
                      % (total, expected_bytes))
    return {'lines': line_number, 'bytes': total}


def _consume(raw, line_number, handle_line, max_line):
    if not raw.strip():
        return
    if len(raw) > max_line:
        raise BadLine(line_number, 'line exceeds byte limit')
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as error:
        raise BadLine(line_number, 'invalid UTF-8') from error
    try:
        document = json.loads(text)
    except ValueError as error:
        raise BadLine(line_number, 'invalid JSON') from error
    if not isinstance(document, dict):
        raise BadLine(line_number, 'line is not a JSON object')
    handle_line(document, line_number)
