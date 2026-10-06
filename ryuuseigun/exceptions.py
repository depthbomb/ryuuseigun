from http import HTTPStatus
from typing import NoReturn, Optional
from ryuuseigun.headers import Headers
from ryuuseigun.constants import StatusCode


class HTTPException(Exception):
    status_code: int
    detail: str
    headers: Headers

    def __init__(self, status_code: int, detail: Optional[str] = None, headers: Optional[dict[str, str]] = None) -> None:
        try:
            default_detail = HTTPStatus(status_code).phrase
        except ValueError:
            default_detail = 'HTTP error'

        self.status_code = status_code
        self.detail = default_detail if detail is None else detail
        self.headers = Headers(headers)
        super().__init__(self.detail)


class ClientDisconnect(HTTPException):
    def __init__(self) -> None:
        super().__init__(StatusCode.BAD_REQUEST, 'Client disconnected before the request body was complete')


def abort(status_code: int, detail: Optional[str] = None, headers: Optional[dict[str, str]] = None) -> NoReturn:
    raise HTTPException(status_code, detail, headers)
