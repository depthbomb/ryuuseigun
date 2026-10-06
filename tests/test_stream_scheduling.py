from ryuuseigun import StreamingResponse
from unittest import IsolatedAsyncioTestCase
from asyncio import Event, sleep, gather, wait_for, all_tasks, create_task, CancelledError

class StreamSchedulingTests(IsolatedAsyncioTestCase):
    async def test_concurrent_empty_and_busy_streams_can_disconnect_and_cancel(self):
        before = all_tasks()
        count = 24
        entered = [Event() for _ in range(count)]
        disconnected = [Event() for _ in range(count)]
        progress = [0] * count
        closed = set()
        after_send = set()

        async def source(index):
            try:
                while True:
                    entered[index].set()
                    progress[index] += 1
                    yield b'' if index % 2 else b'x' * 1024
            finally:
                await sleep(0)
                closed.add(index)

        async def send(message):
            pass

        async def consume(index):
            response = StreamingResponse(source(index))

            @response.after_send
            async def complete():
                after_send.add(index)

            async def receive():
                await disconnected[index].wait()
                return {'type': 'http.disconnect'}

            await response.send(send, receive=receive)

        tasks = [create_task(consume(index)) for index in range(count)]
        try:
            await wait_for(gather(*(event.wait() for event in entered)), 5)
            previous = progress[:]
            await sleep(0)
            await sleep(0)
            self.assertTrue(all(current > earlier for current, earlier in zip(progress, previous, strict=True)))
            for index, task in enumerate(tasks):
                if index % 3:
                    disconnected[index].set()
                else:
                    task.cancel()
            results = await wait_for(gather(*tasks, return_exceptions=True), 5)
            for index, result in enumerate(results):
                if index % 3:
                    self.assertIsNone(result)
                else:
                    self.assertIsInstance(result, CancelledError)
            self.assertEqual(closed, set(range(count)))
            self.assertEqual(after_send, set())
        finally:
            for task in tasks:
                task.cancel()
            await gather(*tasks, return_exceptions=True)
        self.assertEqual(all_tasks(), before)

    async def test_idle_source_closes_on_disconnect_without_another_send(self):
        before = all_tasks()
        entered, disconnected, closed = Event(), Event(), Event()
        messages = []

        async def source():
            try:
                entered.set()
                await Event().wait()
                yield b'unreachable'
            finally:
                closed.set()

        async def receive():
            await disconnected.wait()
            return {'type': 'http.disconnect'}

        async def send(message):
            messages.append(message)

        task = create_task(StreamingResponse(source()).send(send, receive=receive))
        try:
            await wait_for(entered.wait(), 5)
            disconnected.set()
            await wait_for(task, 5)
            self.assertTrue(closed.is_set())
            self.assertEqual([message['type'] for message in messages], ['http.response.start'])
        finally:
            task.cancel()
            await gather(task, return_exceptions=True)
        self.assertEqual(all_tasks(), before)

    async def test_batches_preserve_body_order_finalization_and_trailers(self):
        messages, lifecycle = [], []
        chunks = [b'', 'caf\u00e9', b'a' * 65536, b'b'] * 20

        async def source():
            try:
                for chunk in chunks:
                    yield chunk
            finally:
                lifecycle.append('closed')

        async def send(message):
            messages.append(message)

        response = StreamingResponse(source(), trailers={'X-End': 'done'})

        @response.after_send
        async def complete():
            lifecycle.append('completed')

        await response.send(send, scope={'type': 'http', 'extensions': {'http.response.trailers': {}}})
        bodies = [message for message in messages if message['type'] == 'http.response.body']
        self.assertEqual(b''.join(message['body'] for message in bodies), b''.join(
            chunk.encode() if isinstance(chunk, str) else chunk for chunk in chunks
        ))
        self.assertTrue(all(message['more_body'] for message in bodies[:-1]))
        self.assertEqual(bodies[-1], {'type': 'http.response.body', 'body': b'', 'more_body': False})
        self.assertEqual(messages[-1], {
            'type': 'http.response.trailers', 'headers': [(b'x-end', b'done')], 'more_trailers': False,
        })
        self.assertEqual(lifecycle, ['closed', 'completed'])
