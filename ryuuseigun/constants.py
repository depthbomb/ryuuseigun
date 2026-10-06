from typing import Final, final

__all__ = (
    'ASGIExtension',
    'ASGIMessageType',
    'ASGIScopeType',
    'CacheDirective',
    'ContentEncoding',
    'HeaderName',
    'HTTPMethod',
    'MediaType',
    'RangeUnit',
    'StatusCode',
    'URLScheme',
    'WebSocketCloseCode',
)


@final
class HTTPMethod:
    CONNECT: Final = 'CONNECT'
    DELETE: Final = 'DELETE'
    GET: Final = 'GET'
    HEAD: Final = 'HEAD'
    OPTIONS: Final = 'OPTIONS'
    PATCH: Final = 'PATCH'
    POST: Final = 'POST'
    PUT: Final = 'PUT'
    QUERY: Final = 'QUERY'
    TRACE: Final = 'TRACE'


@final
class StatusCode:
    MINIMUM: Final = 100
    CONTINUE: Final = 100
    SWITCHING_PROTOCOLS: Final = 101
    EARLY_HINTS: Final = 103
    OK: Final = 200
    CREATED: Final = 201
    ACCEPTED: Final = 202
    NO_CONTENT: Final = 204
    PARTIAL_CONTENT: Final = 206
    MULTIPLE_CHOICES: Final = 300
    MOVED_PERMANENTLY: Final = 301
    FOUND: Final = 302
    SEE_OTHER: Final = 303
    NOT_MODIFIED: Final = 304
    TEMPORARY_REDIRECT: Final = 307
    PERMANENT_REDIRECT: Final = 308
    BAD_REQUEST: Final = 400
    UNAUTHORIZED: Final = 401
    FORBIDDEN: Final = 403
    NOT_FOUND: Final = 404
    METHOD_NOT_ALLOWED: Final = 405
    NOT_ACCEPTABLE: Final = 406
    CONFLICT: Final = 409
    GONE: Final = 410
    PRECONDITION_FAILED: Final = 412
    PAYLOAD_TOO_LARGE: Final = 413
    URI_TOO_LONG: Final = 414
    UNSUPPORTED_MEDIA_TYPE: Final = 415
    RANGE_NOT_SATISFIABLE: Final = 416
    UNPROCESSABLE_CONTENT: Final = 422
    TOO_MANY_REQUESTS: Final = 429
    INTERNAL_SERVER_ERROR: Final = 500
    NOT_IMPLEMENTED: Final = 501
    BAD_GATEWAY: Final = 502
    SERVICE_UNAVAILABLE: Final = 503
    GATEWAY_TIMEOUT: Final = 504
    MAXIMUM: Final = 599


@final
class HeaderName:
    ACCEPT: Final = 'Accept'
    ACCEPT_ENCODING: Final = 'Accept-Encoding'
    ACCEPT_QUERY: Final = 'Accept-Query'
    ACCEPT_RANGES: Final = 'Accept-Ranges'
    ALLOW: Final = 'Allow'
    AUTHORIZATION: Final = 'Authorization'
    CACHE_CONTROL: Final = 'Cache-Control'
    CONTENT_DISPOSITION: Final = 'Content-Disposition'
    CONTENT_ENCODING: Final = 'Content-Encoding'
    CONTENT_LENGTH: Final = 'Content-Length'
    CONTENT_LOCATION: Final = 'Content-Location'
    CONTENT_RANGE: Final = 'Content-Range'
    CONTENT_TYPE: Final = 'Content-Type'
    COOKIE: Final = 'Cookie'
    ETAG: Final = 'ETag'
    HOST: Final = 'Host'
    IF_MATCH: Final = 'If-Match'
    IF_MODIFIED_SINCE: Final = 'If-Modified-Since'
    IF_NONE_MATCH: Final = 'If-None-Match'
    IF_RANGE: Final = 'If-Range'
    IF_UNMODIFIED_SINCE: Final = 'If-Unmodified-Since'
    LAST_MODIFIED: Final = 'Last-Modified'
    LINK: Final = 'Link'
    LOCATION: Final = 'Location'
    RANGE: Final = 'Range'
    RETRY_AFTER: Final = 'Retry-After'
    SET_COOKIE: Final = 'Set-Cookie'
    VARY: Final = 'Vary'
    X_ACCEL_BUFFERING: Final = 'X-Accel-Buffering'


@final
class MediaType:
    FORM_URLENCODED: Final = 'application/x-www-form-urlencoded'
    GZIP: Final = 'application/gzip'
    JSON: Final = 'application/json'
    MULTIPART_FORM_DATA: Final = 'multipart/form-data'
    PROBLEM_JSON: Final = 'application/problem+json'
    OCTET_STREAM: Final = 'application/octet-stream'
    ZIP: Final = 'application/zip'
    ZSTANDARD: Final = 'application/zstd'
    TEXT: Final = 'text/plain'
    TEXT_UTF8: Final = 'text/plain; charset=utf-8'
    EVENT_STREAM: Final = 'text/event-stream'
    EVENT_STREAM_UTF8: Final = 'text/event-stream; charset=utf-8'


@final
class ContentEncoding:
    BROTLI: Final = 'br'
    GZIP: Final = 'gzip'
    IDENTITY: Final = 'identity'
    ZSTANDARD: Final = 'zstd'


@final
class CacheDirective:
    NO_CACHE: Final = 'no-cache'


@final
class RangeUnit:
    BYTES: Final = 'bytes'


@final
class URLScheme:
    HTTP: Final = 'http'
    HTTPS: Final = 'https'
    WEBSOCKET: Final = 'ws'
    WEBSOCKET_SECURE: Final = 'wss'


@final
class ASGIScopeType:
    HTTP: Final = 'http'
    LIFESPAN: Final = 'lifespan'
    WEBSOCKET: Final = 'websocket'


@final
class ASGIMessageType:
    HTTP_DISCONNECT: Final = 'http.disconnect'
    HTTP_REQUEST: Final = 'http.request'
    HTTP_RESPONSE_BODY: Final = 'http.response.body'
    HTTP_RESPONSE_EARLY_HINT: Final = 'http.response.early_hint'
    HTTP_RESPONSE_PATHSEND: Final = 'http.response.pathsend'
    HTTP_RESPONSE_START: Final = 'http.response.start'
    HTTP_RESPONSE_TRAILERS: Final = 'http.response.trailers'
    LIFESPAN_SHUTDOWN: Final = 'lifespan.shutdown'
    LIFESPAN_SHUTDOWN_COMPLETE: Final = 'lifespan.shutdown.complete'
    LIFESPAN_SHUTDOWN_FAILED: Final = 'lifespan.shutdown.failed'
    LIFESPAN_STARTUP: Final = 'lifespan.startup'
    LIFESPAN_STARTUP_COMPLETE: Final = 'lifespan.startup.complete'
    LIFESPAN_STARTUP_FAILED: Final = 'lifespan.startup.failed'
    WEBSOCKET_ACCEPT: Final = 'websocket.accept'
    WEBSOCKET_CLOSE: Final = 'websocket.close'
    WEBSOCKET_CONNECT: Final = 'websocket.connect'
    WEBSOCKET_DISCONNECT: Final = 'websocket.disconnect'
    WEBSOCKET_RECEIVE: Final = 'websocket.receive'
    WEBSOCKET_SEND: Final = 'websocket.send'
    WEBSOCKET_RESPONSE_BODY: Final = 'websocket.http.response.body'
    WEBSOCKET_RESPONSE_START: Final = 'websocket.http.response.start'


@final
class ASGIExtension:
    HTTP_EARLY_HINT: Final = ASGIMessageType.HTTP_RESPONSE_EARLY_HINT
    HTTP_PATHSEND: Final = ASGIMessageType.HTTP_RESPONSE_PATHSEND
    HTTP_TRAILERS: Final = ASGIMessageType.HTTP_RESPONSE_TRAILERS
    WEBSOCKET_RESPONSE: Final = 'websocket.http.response'


@final
class WebSocketCloseCode:
    NORMAL: Final = 1000
    GOING_AWAY: Final = 1001
    PROTOCOL_ERROR: Final = 1002
    UNSUPPORTED_DATA: Final = 1003
    POLICY_VIOLATION: Final = 1008
    MESSAGE_TOO_BIG: Final = 1009
    INTERNAL_ERROR: Final = 1011
    APPLICATION_MAX: Final = 4999
