from json import loads
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from time import sleep, perf_counter
from tempfile import TemporaryDirectory
from benchmarks._server import ServerProcess

class ServerTests(TestCase):
    def test_prefix_upload_disconnect_and_lifespan(self):
        with TemporaryDirectory() as directory:
            marker = Path(directory) / 'shutdown.txt'
            with patch.dict('os.environ', {'RYUUSEIGUN_SHUTDOWN_MARKER': str(marker)}):
                with ServerProcess('tests.granian_app:app', root_path='/proxy', health_path='/proxy/health') as server:
                    connection = server.connect()
                    try:
                        connection.request('GET', '/proxy/nested/health')
                        result = loads(connection.getresponse().read())
                        self.assertEqual(result, {'events': ['startup'], 'url': '/proxy/nested/health'})

                        connection.request('POST', '/proxy/nested/upload', body=iter([b'one', b'two']), encode_chunked=True)
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertEqual(loads(response.read()), {'size': 6})

                        connection.request('GET', '/proxy/nested/stream')
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.readline(), b'first\n')
                        response.close()
                    finally:
                        connection.close()

                    deadline = perf_counter() + 5
                    while perf_counter() < deadline:
                        connection = server.connect()
                        try:
                            connection.request('GET', '/proxy/nested/health')
                            events = loads(connection.getresponse().read())['events']
                        finally:
                            connection.close()
                        if events[-1] == 'exit':
                            break
                        sleep(0.05)
                    self.assertEqual(events, ['startup', 'enter', 'generator closed', 'exit'])
                self.assertEqual(marker.read_text(encoding='utf-8'), 'closed')
