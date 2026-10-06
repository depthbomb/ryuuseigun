import asyncio
from time import perf_counter
from ryuuseigun import Ryuuseigun


async def main() -> None:
    app = Ryuuseigun(__name__)

    @app.get('/users/<int:user_id>')
    async def user(_req, user_id: int) -> dict[str, int]:
        return {'id': user_id}

    client = app.test_client()
    iterations = 20_000
    started = perf_counter()
    for _ in range(iterations):
        response = await client.get('/users/42')
        if response.status_code != 200:
            raise RuntimeError('Unexpected benchmark response')
    elapsed = perf_counter() - started
    print(f'{iterations / elapsed:,.0f} in-process requests/second ({elapsed / iterations * 1_000_000:.1f} us/request)')


if __name__ == '__main__':
    asyncio.run(main())
