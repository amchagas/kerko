"""Synchronize the Zotero library into a local cache."""

import json

from flask import current_app
from whoosh.fields import ID, NUMERIC, STORED, Schema

from ..storage import load_object, open_index, save_object
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
        parentItem=ID(stored=True),
        itemType=ID(stored=True),
        data=STORED,
        fulltext=STORED,
    )
    for format_ in get_formats():
        schema.add(format_, STORED)
    return schema


def sync_cache():
    """Build a cache of items retrieved from Zotero."""
    current_app.logger.info("Starting cache sync...")
    count = 0
    zotero_credentials = zotero.init_zotero()
    library_context = zotero.request_library_context(zotero_credentials)  # TODO: Load pickle & sync collections incrementally
    since = load_object('cache', 'version', default=0)
    version = zotero.last_modified_version(zotero_credentials)

    index = open_index('cache', schema=get_cache_schema, auto_create=True, write=True)
    writer = index.writer(limitmb=256)
    try:
        if current_app.config['KERKO_FULLTEXT_SEARCH']:
            fulltext_items = zotero.load_new_fulltext(zotero_credentials, since)
        else:
            fulltext_items = []
        formats = get_formats()
        for item in zotero.Items(zotero_credentials, since=since, formats=list(formats) + ['data']):
            count += 1

            document = {
                'key': item.get('key'),
                'version': item.get('version'),
                'parentItem': item.get('data', {}).get('parentItem', ''),
                'itemType': item.get('data', {}).get('itemType', ''),
                'data': json.dumps(item.get('data', {}))
            }
            for format_ in formats:
                if format_ in item:
                    document[format_] = item[format_]
            if item.get('key') in fulltext_items:
                fulltext = zotero.load_item_fulltext(zotero_credentials, item.get('key'))
                if fulltext:
                    document['fulltext'] = fulltext

            writer.update_document(**document)
            current_app.logger.debug(
                f"Item {count} updated ({item.get('key')}, version {item.get('version')})"
            )

        if since > 0:
            for deleted in zotero.load_deleted_or_trashed_items(zotero_credentials, since):
                count += 1
                writer.delete_by_term('key', deleted)
                current_app.logger.debug(f"Item {count} removed ({deleted})")
    except Exception as e:  # pylint: disable=broad-except
        writer.cancel()
        current_app.logger.exception(e)
        current_app.logger.error('An exception occurred. Could not finish updating the cache.')
    else:
        writer.commit()
        save_object('cache', 'version', version)
        save_object('cache', 'library', library_context)
        current_app.logger.info(
            f"Cache sync successful, now at version {version} ({count} item(s) processed)."
        )
    return count
