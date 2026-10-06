from copy import copy
from typing import Any, Optional
from importlib.util import find_spec
from ryuuseigun.headers import Headers
from ryuuseigun.request import Request
from ryuuseigun.response import Response, FileResponse
from ryuuseigun.constants import MediaType, HeaderName, StatusCode, ContentEncoding


class Compression:
    def __init__(
        self,
        *,
        minimum_size: int = 500,
        encodings: tuple[str, ...] = (ContentEncoding.ZSTANDARD, ContentEncoding.BROTLI, ContentEncoding.GZIP),
    ) -> None:
        if minimum_size < 0:
            raise ValueError('minimum_size must be non-negative')
        unsupported = set(encodings) - {ContentEncoding.BROTLI, ContentEncoding.GZIP, ContentEncoding.ZSTANDARD}
        if unsupported:
            raise ValueError(f'Unsupported content encodings: {", ".join(sorted(unsupported))}')
        self.minimum_size = minimum_size
        self.encodings = tuple(
            encoding
            for encoding in encodings
            if encoding != ContentEncoding.BROTLI or find_spec('brotli') is not None
        )

    async def __call__(self, req: Request[Any], res: Response) -> Response:
        encoding = self._select_encoding(req.headers.get(HeaderName.ACCEPT_ENCODING))
        if _is_compressible(res):
            res = copy(res)
            res.headers = Headers(res.headers)
            if encoding is not None:
                res.enable_compression(encoding, minimum_size=self.minimum_size)
            _append_vary(res, HeaderName.ACCEPT_ENCODING)
        return res

    def _select_encoding(self, header: Optional[str]) -> Optional[str]:
        if not header:
            return None
        qualities: dict[str, float] = {}
        wildcard: Optional[float] = None
        for item in header.split(','):
            name, *parameters = item.strip().lower().split(';')
            quality = 1.0
            for parameter in parameters:
                key, separator, value = parameter.strip().partition('=')
                if key == 'q' and separator:
                    try:
                        quality = max(0.0, min(1.0, float(value)))
                    except ValueError:
                        quality = 0.0
            if name == '*':
                wildcard = quality
            else:
                qualities[name] = quality

        ranked = (
            (qualities.get(encoding, wildcard if wildcard is not None else 0.0), -index, encoding)
            for index, encoding in enumerate(self.encodings)
        )
        quality, _, selected = max(ranked, default=(0.0, 0, ''))
        return selected if quality > 0 else None


def _is_compressible(response: Response) -> bool:
    if (
        isinstance(response, FileResponse)
        or HeaderName.CONTENT_ENCODING in response.headers
        or response.status_code < StatusCode.OK
        or response.status_code in {StatusCode.NO_CONTENT, StatusCode.NOT_MODIFIED}
    ):
        return False
    content_type = response.headers.get(HeaderName.CONTENT_TYPE, '').lower()
    if content_type.startswith(MediaType.EVENT_STREAM):
        return False
    return not content_type.startswith(('image/', 'audio/', 'video/')) and not any(
        value in content_type for value in (MediaType.ZIP, MediaType.GZIP, MediaType.ZSTANDARD)
    )


def _append_vary(response: Response, name: str) -> None:
    current = response.headers.get(HeaderName.VARY)
    if current is None:
        response.headers[HeaderName.VARY] = name
        return
    values = {value.strip().casefold() for value in current.split(',')}
    if name.casefold() not in values:
        response.headers[HeaderName.VARY] = f'{current}, {name}'
