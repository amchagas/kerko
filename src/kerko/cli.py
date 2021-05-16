import pprint
from datetime import datetime

import click
import wrapt
from flask import current_app
from flask.cli import with_appcontext

from .storage import delete_index
from .sync import zotero
from .sync.attachments import delete_attachments, sync_attachments
from .sync.index import sync_index
from .sync.metadata import sync_metadata


@wrapt.decorator
def execution_time_logger(wrapped, _instance, args, kwargs):
    start_time = datetime.now()
    return_value = wrapped(*args, **kwargs)
    current_app.logger.info(_format_elapsed_time(start_time))
    return return_value


@click.group()
def cli():
    """Run a Kerko subcommand."""


@cli.command(deprecated=True)
@with_appcontext
def index():  # Deprecated after version 0.4.
    """
    Synchronize the metadata, the search index, and the attachments.

    This command is deprecated. Use the 'sync' command.
    """
    sync_metadata()
    sync_index()
    sync_attachments()


@cli.command()
@click.argument(
    'target',
    default='everything',
    type=click.Choice(['metadata', 'index', 'attachments', 'everything'], case_sensitive=False),
)
@with_appcontext
@execution_time_logger
def sync(target):
    """
    Synchronize the metadata, the search index and/or the file attachments.

    By default, everything is synchronized.

    Synchronizing everything means that a 3-step process gets executed:

    (1) Synchronize the metadata from the Zotero library into a local metadata
       index.
    (2) Synchronize the search index from the local metadata index.
    (3) Synchronize the file attachments known to the local metadata index.

    Usually, all steps must be executed. But under certain circumstances it can
    be useful to execute a given step individually, e.g., after changing the
    schema of the search index, one can clean and rebuild it from the local
    metadata index.
    """
    if target in ['everything', 'metadata']:
        sync_metadata()
    if target in ['everything', 'index']:
        sync_index()
    if target in ['everything', 'attachments']:
        sync_attachments()


@cli.command()
@click.argument(
    'target',
    default='everything',
    type=click.Choice(['metadata', 'index', 'attachments', 'everything'], case_sensitive=False),
)
@with_appcontext
def clean(target):
    """
    Delete the search index and/or the attachments immediately.

    By default, both are cleaned.
    """
    if target in ['everything', 'metadata']:
        delete_index('metadata')
    if target in ['everything', 'index']:
        delete_index('index')
    if target in ['everything', 'attachments']:
        delete_attachments()


@cli.command()
@click.argument('item_key')
@with_appcontext
def zotero_item(item_key):
    """
    Retrieve an item from the Zotero library.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    pprint.pprint(zotero.load_item(credentials, item_key))


@cli.command()
@with_appcontext
def zotero_item_types():
    """
    List all item types supported by Zotero.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    pprint.pprint(zotero.load_item_types(credentials))


@cli.command()
@with_appcontext
def zotero_item_fields():
    """
    List all fields supported by Zotero.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    pprint.pprint(zotero.load_item_fields(credentials))


@cli.command()
@click.argument('item_type')
@with_appcontext
def zotero_item_type_fields(item_type):
    """
    List all fields supported by Zotero for a given item type.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    pprint.pprint(zotero.load_item_type_fields(credentials, item_type))


@cli.command()
@click.argument('item_type')
@with_appcontext
def zotero_item_type_creator_types(item_type):
    """
    List all creator types supported by Zotero for a given item type.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    pprint.pprint(zotero.load_item_type_creator_types(credentials, item_type))


@cli.command()
@with_appcontext
def zotero_top_level_collections():
    """
    List keys and names of top-level collections from the Zotero library.

    WARNING: This command is provided for debugging purposes only and may be
    modified or removed from the module at any time.
    """
    credentials = zotero.init_zotero()
    collections = zotero.Collections(credentials, top_level=True)
    for c in collections:
        print(f"{c.get('key')} {c.get('data', {}).get('name', '')}")


def _format_elapsed_time(start_time):
    elapsed_time = int(round((datetime.now() - start_time).total_seconds()))
    elapsed_min, elapsed_sec = elapsed_time // 60, elapsed_time % 60
    s = 'Execution time:'
    if elapsed_min > 0:
        s += (' {n} minutes' if elapsed_min > 1 else ' {n} minute').format(n=elapsed_min)
        s += (' {n:02} seconds' if elapsed_sec > 1 else ' {n:02d} second').format(n=elapsed_sec)
    else:
        s += (' {n} seconds' if elapsed_sec > 1 else ' {n} second').format(n=elapsed_sec)
    return s
