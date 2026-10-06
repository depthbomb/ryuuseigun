from uuid import uuid4
from email.message import Message
from ryuuseigun.headers import Headers
from urllib.parse import urlsplit, urlencode
from collections.abc import Mapping, Sequence
from ryuuseigun.constants import MediaType, HeaderName

type FormFields = Mapping[str, str] | Sequence[tuple[str, str]]
type FileField = tuple[str, bytes, str]
type FileFields = Mapping[str, FileField] | Sequence[tuple[str, FileField]]

class CookieResponse:
    def __init__(self, headers: Headers) -> None:
        self.message = Message()
        for value in headers.getlist(HeaderName.SET_COOKIE):
            self.message[HeaderName.SET_COOKIE] = value

    def info(self) -> Message:
        return self.message

def origin(url: str) -> tuple[str, str, int]:
    parts = urlsplit(url)
    if parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password:
        raise ValueError('Test URLs must use HTTP or HTTPS with a host and no credentials')
    return parts.scheme, parts.hostname, parts.port if parts.port is not None else (443 if parts.scheme == 'https' else 80)

def _quoted(value: str) -> str:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError('Invalid multipart field name or filename')
    return value.replace('\\', '\\\\').replace('"', '\\"')

def encode_form(form: FormFields | None, files: FileFields | None, headers: Headers) -> bytes:
    fields = list(form.items() if isinstance(form, Mapping) else form or ())
    if files is None:
        headers.setdefault(HeaderName.CONTENT_TYPE, MediaType.FORM_URLENCODED)
        return urlencode(fields).encode('utf-8')
    if HeaderName.CONTENT_TYPE in headers:
        raise ValueError('Multipart Content-Type is generated with its boundary; omit that header')
    boundary = uuid4().hex
    headers[HeaderName.CONTENT_TYPE] = f'multipart/form-data; boundary={boundary}'
    chunks: list[bytes] = []
    for name, value in fields:
        chunks.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{_quoted(name)}"\r\n\r\n'.encode()
        )
        chunks.extend((value.encode('utf-8'), b'\r\n'))
    uploads = files.items() if isinstance(files, Mapping) else files
    for name, (filename, content, media_type) in uploads:
        Headers({HeaderName.CONTENT_TYPE: media_type})
        chunks.append(
            (f'--{boundary}\r\nContent-Disposition: form-data; name="{_quoted(name)}"; '
             f'filename="{_quoted(filename)}"\r\nContent-Type: {media_type}\r\n\r\n').encode()
        )
        chunks.extend((content, b'\r\n'))
    chunks.append(f'--{boundary}--\r\n'.encode())
    return b''.join(chunks)
