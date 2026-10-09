from unittest import IsolatedAsyncioTestCase
from examples.composition import create_app

class TypedStateTests(IsolatedAsyncioTestCase):
    async def test_services_and_connection_state_are_isolated(self):
        first, second = create_app('one'), create_app('two')
        async with first.test_client() as one, second.test_client() as two:
            self.assertEqual((await one.get('/feature')).json(), {'service': 'one', 'ready': True, 'visited': True})
            self.assertEqual((await two.get('/feature')).json()['service'], 'two')
            for client in (one, one, two):
                async with client.websocket('/socket') as socket:
                    self.assertEqual((await socket.receive_json())['messages'], 1)
        self.assertFalse(first.state.ready)
        self.assertFalse(second.state.ready)
