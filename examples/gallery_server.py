from os import environ
from examples.gallery.app import create_app

app = create_app(
    password=environ['GALLERY_PASSWORD'],
    database_path=environ.get('GALLERY_DATABASE_PATH', 'gallery.db'),
    allowed_origin=environ.get('GALLERY_ORIGIN', 'https://localhost'),
    secure_cookies=environ.get('GALLERY_INSECURE_LOCAL_COOKIES') != '1',
)
