from types import SimpleNamespace
from ryuuseigun.request import Request
from ryuuseigun.config import MultipartLimits
from ryuuseigun.exceptions import HTTPException
from unittest import IsolatedAsyncioTestCase

def make_request(headers, payload=b'payload', *, chunk_size=4096, limits=None):
    body = b'--boundary\r\n' + headers + b'\r\n\r\n' + payload + b'\r\n--boundary--\r\n'
    position = 0

    async def receive():
        nonlocal position
        chunk = body[position:position + chunk_size]
        position += len(chunk)
        return {'type': 'http.request', 'body': chunk, 'more_body': position < len(body)}

    return Request(
        {'type': 'http', 'method': 'POST', 'path': '/',
         'headers': [(b'content-type', b'multipart/form-data; boundary=boundary')]},
        receive, state=SimpleNamespace(), multipart_limits=limits,
    )

class MultipartHeaderTests(IsolatedAsyncioTestCase):
    async def test_plain_headers_preserve_case_whitespace_and_empty_filename(self):
        disposition = b'FoRm-DaTa; NaMe=" a;b "; FiLeNaMe="" \t'
        headers = b'cOnTent-DiSpoSition:\t' + disposition + b'\r\ncOnTent-TyPe: TEXT/PLAIN \t'
        for chunk_size in (1, 7, 4096):
            with self.subTest(chunk_size=chunk_size):
                req = make_request(headers, chunk_size=chunk_size)
                try:
                    part = await anext(req.multipart())
                    self.assertEqual((part.name, part.filename, part.content_type), (' a;b ', '', 'text/plain'))
                    self.assertEqual(list(part.headers.items()), [
                        ('cOnTent-DiSpoSition', disposition.decode()), ('cOnTent-TyPe', 'TEXT/PLAIN \t'),
                    ])
                    self.assertEqual(await part.read(), b'payload')
                    with self.assertRaises(StopAsyncIteration):
                        await anext(req.multipart())
                finally:
                    await req.close()

    async def test_encoded_and_escaped_parameters_keep_mime_decoding(self):
        cases = [
            (b'name="file"; filename="=?utf-8?b?Y2Fmw6k=?="', 'file', 'caf\u00e9'),
            (b'name="=?utf-8?q?caf=C3=A9?="; filename="x"', 'caf\u00e9', 'x'),
            (b"name=file; filename*=utf-8''caf%C3%A9.txt", 'file', 'caf\u00e9.txt'),
            (b"name=file; filename*0*=utf-8''long%20; filename*1*=name.txt", 'file', 'long name.txt'),
            (b'name="file"; filename="caf\xc3\xa9.txt"', 'file', 'caf\u00e9.txt'),
            (b'name="quo\\\"ted"; filename="a\\\\b.txt"', 'quo"ted', 'a\\b.txt'),
            (b'name="first"; name="second"; filename="first"; filename="second"', 'first', 'first'),
        ]
        for parameters, name, filename in cases:
            with self.subTest(parameters=parameters):
                disposition = b'form-data; ' + parameters
                req = make_request(b'Content-Disposition: ' + disposition)
                try:
                    part = await anext(req.multipart())
                    self.assertEqual((part.name, part.filename, part.content_type), (name, filename, None))
                    self.assertEqual(part.headers['Content-Disposition'].encode('latin-1'), disposition)
                    self.assertEqual(await part.read(), b'payload')
                finally:
                    await req.close()

    async def test_reordered_folded_headers_keep_charset_and_duplicates(self):
        req = make_request(
            b'Content-Type: text/plain; charset=iso-8859-1\r\n'
            b'Content-Disposition: form-data;\r\n name="field"\r\n'
            b'X-Note: first\r\n second\r\nX-Note: last', b'caf\xe9',
        )
        try:
            part = await anext(req.multipart())
            self.assertEqual(part.name, 'field')
            self.assertEqual(part.content_type, 'text/plain')
            self.assertEqual(part.headers.getlist('X-Note'), ['first second', 'last'])
            self.assertEqual(await part.text(), 'caf\u00e9')
        finally:
            await req.close()

    async def test_plain_headers_still_enforce_limits_and_reject_invalid_input(self):
        plain = b'Content-Disposition: form-data; name="field"'
        file = plain + b'; filename="data.bin"'
        cases = [
            (plain, MultipartLimits(max_parts=0), 413),
            (plain, MultipartLimits(max_field_size=3), 413),
            (file, MultipartLimits(max_file_size=3), 413),
            (plain + b'\r\nContent-Type: multipart/mixed', None, 400),
            (plain + b'\r\nContent-Transfer-Encoding: base64', None, 400),
            (b'Content-Disposition: form-data; name=""', None, 400),
            (b'Content-Disposition: form-data; name="a\x00b"', None, 400),
            (b'Content-Disposition: form-data; name="' + b'x' * 65536 + b'"', None, 400),
        ]
        for headers, limits, status in cases:
            with self.subTest(headers=headers[:100], limits=limits):
                req = make_request(headers, limits=limits)
                try:
                    with self.assertRaises(HTTPException) as caught:
                        await req.form()
                    self.assertEqual(caught.exception.status_code, status)
                finally:
                    await req.close()

    async def test_header_values_stay_local_across_requests_and_cache_eviction(self):
        first = make_request(b'Content-Disposition: form-data; name="file"; filename="first"\r\nX-First: one')
        try:
            original = await anext(first.multipart())
            for index in range(260):
                req = make_request(
                    f'Content-Disposition: form-data; name="file"; filename="file-{index}"\r\n'
                    f'X-Unique-{index}: value-{index}'.encode(),
                )
                try:
                    part = await anext(req.multipart())
                    self.assertEqual(part.filename, f'file-{index}')
                    self.assertEqual(part.headers[f'X-Unique-{index}'], f'value-{index}')
                    part.headers['Content-Disposition'] = 'changed'
                    self.assertEqual(await part.read(), b'payload')
                finally:
                    await req.close()
            self.assertEqual(original.filename, 'first')
            self.assertEqual(original.headers['Content-Disposition'], 'form-data; name="file"; filename="first"')
            self.assertEqual(original.headers['X-First'], 'one')
            self.assertEqual(await original.read(), b'payload')
        finally:
            await first.close()
