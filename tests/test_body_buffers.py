from types import SimpleNamespace
from ryuuseigun import Request
from unittest import IsolatedAsyncioTestCase

def make_request(body, *, multipart=False, chunk_size=65536):
    chunks = [body[index:index + chunk_size] for index in range(0, len(body), chunk_size)] or [b'']
    messages = iter([
        {'type': 'http.request', 'body': chunk, 'more_body': index < len(chunks) - 1}
        for index, chunk in enumerate(chunks)
    ])

    async def receive():
        return next(messages)

    headers = [(b'content-type', b'multipart/form-data; boundary=boundary')] if multipart else []
    return Request(
        {'type': 'http', 'method': 'POST', 'path': '/', 'headers': headers},
        receive, state=SimpleNamespace(),
    )

class BufferedReadTests(IsolatedAsyncioTestCase):
    async def test_body_preserves_bytes_and_caches_the_completed_buffer(self):
        for size in (0, 1, 65537, 1048593):
            with self.subTest(size=size):
                payload = (bytes(range(256)) * (size // 256 + 1))[:size]
                req = make_request(payload)
                try:
                    body = await req.body()
                    self.assertIs(type(body), bytes)
                    self.assertEqual(body, payload)
                    self.assertIs(await req.body(), body)
                finally:
                    await req.close()
                self.assertEqual(body, payload)

    async def test_part_bytes_survive_following_parts_and_request_cleanup(self):
        for size in (0, 1, 65537, 1048593):
            with self.subTest(size=size):
                payload = (bytes(range(256)) * (size // 256 + 1))[:size]
                wire_body = (
                    b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="data.bin"\r\n\r\n'
                    + payload + b'\r\n--boundary\r\nContent-Disposition: form-data; name="tail"\r\n\r\nafter'
                    b'\r\n--boundary--\r\n'
                )
                req = make_request(wire_body, multipart=True)
                try:
                    stream = req.multipart()
                    first = await anext(stream)
                    body = await first.read()
                    self.assertIs(type(body), bytes)
                    self.assertEqual(body, payload)
                    self.assertTrue(first.complete)
                    self.assertEqual(first.size, size)
                    with self.assertRaisesRegex(RuntimeError, 'already been consumed'):
                        await first.read()
                    tail = await anext(stream)
                    self.assertEqual(await tail.read(), b'after')
                    with self.assertRaises(StopAsyncIteration):
                        await anext(stream)
                finally:
                    await req.close()
                self.assertEqual(body, payload)
