"""Optional Pydantic validation. Import explicitly with the validation extra installed."""
from typing import Any
from ryuuseigun.app import Ryuuseigun
from ryuuseigun.request import Request
from ryuuseigun.response import JSONResponse
from pydantic import BaseModel, ValidationError
from ryuuseigun.exceptions import HTTPException
from ryuuseigun.constants import MediaType, HeaderName, StatusCode

class RequestValidationError(HTTPException):
    def __init__(self, error: ValidationError) -> None:
        super().__init__(StatusCode.UNPROCESSABLE_CONTENT, 'Request validation failed')
        self.errors = error.errors(include_url=False, include_context=False, include_input=False)

def install_validation(app: Ryuuseigun[Any]) -> None:
    """Install structured validation errors without replacing an application handler."""
    if RequestValidationError in app.error_handlers:
        return

    @app.errorhandler(RequestValidationError)
    async def invalid_request(req: Request[Any], error: RequestValidationError) -> JSONResponse:
        return JSONResponse({'errors': error.errors}, status_code=StatusCode.UNPROCESSABLE_CONTENT)

async def parse_json[Model: BaseModel](req: Request[Any], model: type[Model]) -> Model:
    """Validate a JSON body explicitly, reusing any schema-decorator validation."""
    cache = req.scope.setdefault('ryuuseigun.validated', {})
    key = ('body', model)
    cached = cache.get(key)
    if isinstance(cached, model):
        return cached
    media_type = req.headers.get(HeaderName.CONTENT_TYPE, '').partition(';')[0].strip().lower()
    if media_type != MediaType.JSON:
        raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, 'Expected application/json')
    payload = await req.json()
    try:
        value = model.model_validate(payload)
    except ValidationError as error:
        raise RequestValidationError(error) from error
    cache[key] = value
    return value

def parse_query[Model: BaseModel](req: Request[Any], model: type[Model]) -> Model:
    """Validate scalar query fields; repeated values require an explicit list field."""
    cache = req.scope.setdefault('ryuuseigun.validated', {})
    key = ('query', model)
    cached = cache.get(key)
    if isinstance(cached, model):
        return cached
    values: dict[str, Any] = dict(req.query.items())
    for name in values:
        repeated = req.query.getlist(name)
        if len(repeated) > 1:
            values[name] = repeated
    try:
        value = model.model_validate(values)
    except ValidationError as error:
        raise RequestValidationError(error) from error
    cache[key] = value
    return value
