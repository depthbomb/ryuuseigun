"""The gallery's SQLite schema. Regenerate models and migrations after edits."""
from sqrrl.schema import blob, real, text, Table, Schema, integer

schema = Schema(tables=(
    Table('images', model='Image', fields=(
        integer('id').primary_key(),
        text('title'),
        blob('content').nullable(),
    )),
    Table('sessions', model='Session', fields=(
        text('token').primary_key(),
        text('csrf'),
        real('expires'),
    )),
))
