"""Update the search index from the local cache."""

import whoosh
from flask import current_app

from ..extractors import ItemContext
from ..storage import open_index
from ..tags import TagGate
from . import zotero


def sync_index():
    """Build the search index from items retrieved from Zotero."""
    current_app.logger.info("Starting index sync...")
    composer = current_app.config['KERKO_COMPOSER']
    zotero_credentials = zotero.init_zotero()
    library_context = zotero.request_library_context(zotero_credentials)
    index = open_index(
        'index', schema=current_app.config['KERKO_COMPOSER'].schema, auto_create=True, write=True
    )
    count = 0

    def get_children(item):
        children = []
        if item.get('meta', {}).get('numChildren', 0):
            # TODO: Extract just the item types that are required by the Composer instance's fields.
            children = list(
                zotero.ChildItems(
                    zotero_credentials,
                    item['key'],
                    item_types=['note', 'attachment'],
                    fulltext=current_app.config['KERKO_FULLTEXT_SEARCH']
                )
            )
        return children

    writer = index.writer(limitmb=256)
    try:
        writer.mergetype = whoosh.writing.CLEAR
        allowed_item_types = [
            t for t in library_context.item_types.keys()
            if t not in ['note', 'attachment']
        ]
        formats = {
            spec.extractor.format
            for spec in list(composer.fields.values()) + list(composer.facets.values())
        }
        gate = TagGate(composer.default_item_include_re, composer.default_item_exclude_re)
        for item in zotero.Items(zotero_credentials, 0, allowed_item_types, list(formats)):
            count += 1
            if gate.check(item.get('data', {})):
                item_context = ItemContext(item, get_children(item))
                document = {}
                for spec in list(composer.fields.values()) + list(composer.facets.values()):
                    spec.extract_to_document(document, item_context, library_context)
                update_document_with_writer(writer, document, count=count)
            else:
                current_app.logger.debug(f"Document {count} excluded ({item['key']})")
    except Exception as e:  # pylint: disable=broad-except
        writer.cancel()
        current_app.logger.exception(e)
        current_app.logger.error('An exception occurred. Could not finish updating the index.')
    else:
        writer.commit()
        current_app.logger.info(f"Index sync successful ({count} item(s) processed).")
    return count


def update_document_with_writer(writer, document, count=None):
    """
    Update a document in the search index.

    :param writer: The index writer.

    :param document: A dict whose fields match the schema.

    :param count: An optional document count, for logging purposes.
    """
    writer.update_document(**document)
    current_app.logger.debug(
        'Document {count}updated ({id}): {title}'.format(
            id=document.get('id', ''),
            title=document.get('z_title'),
            count='' if count is None else '{} '.format(count)
        )
    )


def update_document(document):
    """
    Update a document in the search index.

    :param document: A dict whose fields match the schema.
    """
    index = open_index('index', write=True)
    with index.writer(limitmb=256) as writer:
        update_document_with_writer(writer, document)
