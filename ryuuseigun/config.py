from enum import Enum
from typing import Optional
from dataclasses import dataclass
from ryuuseigun.headers import Headers
from ryuuseigun.constants import MediaType
from orjson import (
    OPT_UTC_Z,
    OPT_INDENT_2,
    OPT_NAIVE_UTC,
    OPT_SORT_KEYS,
    OPT_NON_STR_KEYS,
    OPT_APPEND_NEWLINE,
    OPT_STRICT_INTEGER,
    OPT_SERIALIZE_NUMPY,
    OPT_OMIT_MICROSECONDS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_PASSTHROUGH_SUBCLASS,
    OPT_PASSTHROUGH_DATACLASS,
)

_ORJSON_OPTIONS = (
    OPT_APPEND_NEWLINE
    | OPT_INDENT_2
    | OPT_NAIVE_UTC
    | OPT_NON_STR_KEYS
    | OPT_OMIT_MICROSECONDS
    | OPT_PASSTHROUGH_DATACLASS
    | OPT_PASSTHROUGH_DATETIME
    | OPT_PASSTHROUGH_SUBCLASS
    | OPT_SERIALIZE_NUMPY
    | OPT_SORT_KEYS
    | OPT_STRICT_INTEGER
    | OPT_UTC_Z
)


def _validate_optional_limit(name: str, value: Optional[int]) -> None:
    if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
        raise ValueError(f'{name} must be a non-negative integer or None')


@dataclass(slots=True, frozen=True)
class MultipartLimits:
    max_parts: Optional[int] = 1000
    max_field_size: Optional[int] = 1024 * 1024
    max_file_size: Optional[int] = None

    def __post_init__(self) -> None:
        _validate_optional_limit('max_parts', self.max_parts)
        _validate_optional_limit('max_field_size', self.max_field_size)
        _validate_optional_limit('max_file_size', self.max_file_size)


class _Inherit(Enum):
    DEFAULT = 'inherit'


@dataclass(slots=True, frozen=True)
class MultipartOverrides:
    """Per-route limits: omitted fields inherit; None disables that limit."""

    max_parts: int | None | _Inherit = _Inherit.DEFAULT
    max_field_size: int | None | _Inherit = _Inherit.DEFAULT
    max_file_size: int | None | _Inherit = _Inherit.DEFAULT

    def __post_init__(self) -> None:
        for name in ('max_parts', 'max_field_size', 'max_file_size'):
            value = getattr(self, name)
            if not isinstance(value, _Inherit):
                _validate_optional_limit(name, value)


def resolve_multipart_limits(options: Optional[MultipartOverrides], defaults: MultipartLimits) -> MultipartLimits:
    if options is None:
        return defaults
    if not isinstance(options, MultipartOverrides):
        raise TypeError('Route multipart settings must be MultipartOverrides')

    return MultipartLimits(
        max_parts=defaults.max_parts if isinstance(options.max_parts, _Inherit) else options.max_parts,
        max_field_size=defaults.max_field_size if isinstance(options.max_field_size, _Inherit) else options.max_field_size,
        max_file_size=defaults.max_file_size if isinstance(options.max_file_size, _Inherit) else options.max_file_size,
    )


@dataclass(slots=True, frozen=True)
class Config:
    debug: bool = False
    strict_slashes: bool = False
    redirect_slashes: bool = False
    pretty_json: bool = False
    json_options: int = 0
    max_request_body_size: Optional[int] = 16 * 1024 * 1024
    upload_spool_threshold: int = 1024 * 1024
    max_multipart_parts: Optional[int] = 1000
    max_multipart_field_size: Optional[int] = 1024 * 1024
    max_multipart_file_size: Optional[int] = None
    max_query_string_size: Optional[int] = 8 * 1024
    max_query_parameters: Optional[int] = 1000
    automatic_head: bool = True
    automatic_options: bool = True
    trusted_hosts: tuple[str, ...] = ()
    propagate_exceptions: bool = False
    expose_error_details: bool = False
    error_media_type: str = MediaType.JSON
    default_response_headers: tuple[tuple[str, str], ...] = ()
    websocket_auto_close: bool = True

    @property
    def multipart_limits(self) -> MultipartLimits:
        return MultipartLimits(
            max_parts=self.max_multipart_parts,
            max_field_size=self.max_multipart_field_size,
            max_file_size=self.max_multipart_file_size,
        )

    def __post_init__(self) -> None:
        limits = {
            'max_request_body_size': self.max_request_body_size,
            'max_query_string_size': self.max_query_string_size,
            'max_query_parameters': self.max_query_parameters,
            'max_multipart_parts': self.max_multipart_parts,
            'max_multipart_field_size': self.max_multipart_field_size,
            'max_multipart_file_size': self.max_multipart_file_size,
        }
        for name, value in limits.items():
            _validate_optional_limit(name, value)

        if (
            not isinstance(self.upload_spool_threshold, int)
            or isinstance(self.upload_spool_threshold, bool)
            or self.upload_spool_threshold < 0
        ):
            raise ValueError('upload_spool_threshold must be non-negative')

        if not isinstance(self.json_options, int) or isinstance(self.json_options, bool) or self.json_options < 0:
            raise ValueError('json_options must be a non-negative integer')
        if self.json_options & ~_ORJSON_OPTIONS:
            raise ValueError('json_options contains unsupported orjson options')

        if self.error_media_type not in {MediaType.JSON, MediaType.PROBLEM_JSON}:
            raise ValueError(f'error_media_type must be {MediaType.JSON!r} or {MediaType.PROBLEM_JSON!r}')

        normalized_hosts: set[str] = set()
        for host in self.trusted_hosts:
            normalized = host.casefold().rstrip('.')
            if not normalized or any(character.isspace() for character in normalized) or any(
                character in normalized for character in '/\\@'
            ):
                raise ValueError(f'Invalid trusted host: {host!r}')
            if ':' in normalized and not (normalized.startswith('[') and normalized.endswith(']')):
                raise ValueError(f'Invalid trusted host: {host!r}')
            if normalized != '*' and '*' in normalized and (
                normalized == '*.' or not normalized.startswith('*.') or normalized.count('*') != 1
            ):
                raise ValueError(f'Invalid trusted host: {host!r}')
            normalized_hosts.add(normalized)
        if len(normalized_hosts) != len(self.trusted_hosts):
            raise ValueError('trusted_hosts cannot contain duplicates')

        headers = Headers(list(self.default_response_headers))
        if len(headers) != len(self.default_response_headers):
            raise ValueError('default_response_headers cannot contain duplicate names')
