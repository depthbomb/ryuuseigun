from os import environ
from examples.gallery import create_app

app = create_app(
    password=environ['GALLERY_PASSWORD'],
    database_url=environ.get('GALLERY_DATABASE_URL', 'sqlite+aiosqlite:///gallery.db'),
    allowed_origin=environ.get('GALLERY_ORIGIN', 'https://localhost'),
    secure_cookies=environ.get('GALLERY_INSECURE_LOCAL_COOKIES') != '1',
)
