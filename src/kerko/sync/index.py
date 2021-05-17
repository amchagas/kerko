"""Update the search index from the local cache."""

import json

import whoosh
from flask import current_app
from whoosh.query import Every, Term

from ..storage import load_object, open_index, save_object
from ..tags import TagGate


def sync_index():
    """Build the search index from the local cache."""

    current_app.logger.info("Starting index sync...")
    composer = current_app.config['KERKO_COMPOSER']
    library_context = load_object('cache', 'library')
    cache = open_index('cache')
    cache_version = load_object('cache', 'version', default=0)

    if not cache_version:
        current_app.logger.error("The cache is empty and needs to be synchronized first.")
        return 0
    if load_object('index', 'version', default=0) == cache_version:
        current_app.logger.warning("The index is already up-to-date with the cache, nothing to do.")
        return 0

    def yield_items(parent_key):
        with cache.searcher() as searcher:
            results = searcher.search(Every(), filter=Term('parent_item', parent_key), limit=None)
            if results:
                for hit in results:
                    item = hit.fields()
                    item['data'] = json.loads(item['data'])
                    yield item

    def yield_top_level_items():
        return yield_items('')

    def yield_children(parent):
        return yield_items(parent['key'])

    count = 0
    index = open_index('index', schema=composer.schema, auto_create=True, write=True)
    writer = index.writer(limitmb=256)
    try:
        writer.mergetype = whoosh.writing.CLEAR
        gate = TagGate(composer.default_item_include_re, composer.default_item_exclude_re)
        for item in yield_top_level_items():
            count += 1
            if gate.check(item['data']):
                item['children'] = list(yield_children(item))  # Extend the base Zotero item dict.
                document = {}
                for spec in list(composer.fields.values()) + list(composer.facets.values()):
                    spec.extract_to_document(document, item, library_context)
                writer.update_document(**document)
                current_app.logger.debug(
                    f"Item {count} updated ({document.get('id', '')}): {document.get('z_title')}"
                )
            else:
                current_app.logger.debug(f"Item {count} excluded ({item['key']})")
    except Exception as e:  # pylint: disable=broad-except
        writer.cancel()
        current_app.logger.exception(e)
        current_app.logger.error('An exception occurred. Could not finish updating the index.')
    else:
        writer.commit()
        save_object('index', 'version', cache_version)
        current_app.logger.info(
            f"Index sync successful, now at version {cache_version} ({count} item(s) processed)."
        )
    return count


# def sync_index():  # FIXME: Remove!
#     """Build the search index from items retrieved from Zotero."""
#     current_app.logger.info("Starting index sync...")
#     composer = current_app.config['KERKO_COMPOSER']
#     zotero_credentials = zotero.init_zotero()
#     library_context = zotero.request_library_context(zotero_credentials)
#     index = open_index(
#         'index', schema=current_app.config['KERKO_COMPOSER'].schema, auto_create=True, write=True
#     )
#     count = 0
#
#     def get_children(item):
#         children = []
#         if item.get('meta', {}).get('numChildren', 0):
#             # TODO: Extract just the item types that are required by the Composer instance's fields.
#             children = list(
#                 zotero.ChildItems(
#                     zotero_credentials,
#                     item['key'],
#                     item_types=['note', 'attachment'],
#                     fulltext=current_app.config['KERKO_FULLTEXT_SEARCH']
#                 )
#             )
#         return children
#
#     writer = index.writer(limitmb=256)
#     try:
#         writer.mergetype = whoosh.writing.CLEAR
#         allowed_item_types = [
#             t for t in library_context.item_types.keys()
#             if t not in ['note', 'attachment']
#         ]
#         formats = {
#             spec.extractor.format
#             for spec in list(composer.fields.values()) + list(composer.facets.values())
#         }
#         gate = TagGate(composer.default_item_include_re, composer.default_item_exclude_re)
#         for item in zotero.Items(zotero_credentials, 0, allowed_item_types, list(formats)):
#             count += 1
#             if gate.check(item.get('data', {})):
#                 item_context = ItemContext(item, get_children(item))
#                 document = {}
#                 for spec in list(composer.fields.values()) + list(composer.facets.values()):
#                     spec.extract_to_document(document, item_context, library_context)
#                 update_document_with_writer(writer, document, count=count)
#             else:
#                 current_app.logger.debug(f"Document {count} excluded ({item['key']})")
#     except Exception as e:  # pylint: disable=broad-except
#         writer.cancel()
#         current_app.logger.exception(e)
#         current_app.logger.error('An exception occurred. Could not finish updating the index.')
#     else:
#         writer.commit()
#         current_app.logger.info(f"Index sync successful ({count} item(s) processed).")
#     return count
