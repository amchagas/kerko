"""Synchronize the Zotero library into a local cache."""

import json

import whoosh
from flask import current_app
from whoosh.fields import ID, NUMERIC, STORED, Schema

from ..storage import open_index
from ..tags import TagGate
from . import zotero


def get_formats():
    composer = current_app.config['KERKO_COMPOSER']
    return {
        spec.extractor.format
        for spec in list(composer.fields.values()) + list(composer.facets.values())
        if spec.extractor.format != 'data'
    }


def get_cache_schema():
    schema = Schema(
        key=ID(unique=True, stored=True),
        version=NUMERIC(stored=True),
        parent_item=ID(stored=True),
        item_type=ID(stored=True),
        data=STORED,
        fulltext=STORED,
    )
    for format_ in get_formats():
        schema.add(format_, STORED)
    return schema


def sync_cache():
    """Build a cache of items retrieved from Zotero."""
    current_app.logger.info("Starting cache sync...")
    composer = current_app.config['KERKO_COMPOSER']
    zotero_credentials = zotero.init_zotero()
    library_context = zotero.request_library_context(zotero_credentials)
    index = open_index('cache', schema=get_cache_schema, auto_create=True, write=True)
    count = 0
    since = 0  # FIXME: Read 'since' value from last sync.
    version = zotero.last_modified_version(zotero_credentials)  # FIXME: Save version after sync.

    writer = index.writer(limitmb=256)
    try:
        writer.mergetype = whoosh.writing.CLEAR
        allowed_item_types = [
            t for t in library_context.item_types.keys()
            if t not in ['note', 'attachment']
        ]
        formats = get_formats()
        gate = TagGate(composer.default_item_include_re, composer.default_item_exclude_re)
        for item in zotero.Items(
            zotero_credentials, since, allowed_item_types,
            list(formats) + ['data']
        ):
            # FIXME: If list of fulltext items not known yet and current_app.config['KERKO_FULLTEXT_SEARCH'], retrieve it
            count += 1
            if gate.check(item.get('data', {})):
                document = {
                    'key': item.get('key'),
                    'version': item.get('version'),
                    'parent_item': item.get('data', {}).get('parentItem', ''),
                    'item_type': item.get('data', {}).get('itemType', ''),
                    'data': json.dumps(item.get('data', {}))
                }
                for format_ in formats:
                    document[format_] = item.get(format_, '')
                # FIXME: if we have a list of fulltext items, check if item key is in it, if so retrieve its the fulltext and add it to document
                writer.update_document(**document)
                current_app.logger.debug(f"Item {count} updated ({item.get('key')}, version {item.get('version')})")
            else:
                current_app.logger.debug(f"Item {count} excluded ({item.get('key')})")  # FIXME: in the context of incremental sync, should delete from index if it exists. But newly included items won't be added if they are older than `since`. Need a clean+sync if inclusion rules change.

        # FIXME: Retrieve notes and attachments with zotero.Items, including
        # standalone notes. But nah, it should not be through a separate
        # request!! Otherwise the version might be different! Should avoid
        # excluding types from `allowed_item_types`, but process the items
        # differently when they are of type 'note' or 'attachment, which won't
        # have the format fields.
    except Exception as e:  # pylint: disable=broad-except
        writer.cancel()
        current_app.logger.exception(e)
        current_app.logger.error('An exception occurred. Could not finish updating the cache.')
    else:
        writer.commit()
        current_app.logger.info(
            f"Cache sync successful, now at version {version} ({count} item(s) processed)."
        )
    return count
