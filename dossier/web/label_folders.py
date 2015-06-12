'''
Foldering support using :mod:`dossier.store` and :mod:`dossier.label`
=====================================================================

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: Folders

N.B. This is deprecated and should not be used.
'''
from __future__ import absolute_import, division, print_function

from collections import defaultdict
from itertools import groupby, imap
import logging
import urllib

from dossier.fc import FeatureCollection
from dossier.label import CorefValue, Label

logger = logging.getLogger(__name__)


class Folders(object):
    '''Provides basic foldering support by using labels.

    :class:`Folders` implements a simple abstraction for
    storing folders in a :class:`dossier.fc.Store` and a
    :class:`dossier.label.LabelStore`. A fixed hierarchy of three
    levels is supported: folders contain subfolders and subfolders
    contain items.

    Folders and subfolders each have an identifier and its name is
    derived from that identifier. The identifier scheme is similar to
    what MediaWiki uses. If `The_Id` is the identifier, then `The Id`
    is the name (and vice versa). Note that the ASCII space character
    (``0x20``) and the ASCII slash character (``0x2F``) are not allowed
    in folder identifiers.

    Folders may be empty. Subfolders *cannot* be empty.

    Items in a subfolder are pairs of `(content_id, subtopic_id)`.
    The precise form of `content_id` and `subtopic_id` is not
    specified (it is application specific).

    Folders cannot be deleted or modified.

    .. automethod:: id_to_name
    .. automethod:: name_to_id
    .. automethod:: __init__
    .. automethod:: add_folder
    .. automethod:: add_item
    .. automethod:: folders
    .. automethod:: subfolders
    .. automethod:: items
    .. automethod:: parent_subfolders
    '''
    DEFAULT_ANNOTATOR_ID = 'unknown'

    @staticmethod
    def id_to_name(ident):
        'Converts a folder id to a folder name.'
        if ident is None:
            return ident
        else:
            return ident.replace('_', ' ')

    @staticmethod
    def name_to_id(name):
        'Converts a folder name to a folder id.'
        if name is None:
            return name
        else:
            return name.replace(' ', '_')

    def __init__(self, store, label_store, prefix=''):
        '''Create a new :class:`Folders` instance.

        :param store: An FC store
        :type store: :class:`dossier.store.Store`
        :param label_store: A label store
        :type label_store: :class:`dossier.label.LabelStore`
        :param prefix: A folder id prefix
        :type prefix: unicode
        :rtype: :class:`Folders`
        '''
        self.store = store
        self.label_store = label_store
        self.prefix = prefix.encode('utf-8')

    def folders(self, ann_id=None):
        '''Yields an unordered generator for all available folders.

        By default (with ``ann_id=None``), folders are shown for all
        anonymous users. Optionally, ``ann_id`` can be set to a username,
        which restricts the list to only folders owned by that user.

        :param str ann_id: Username
        :rtype: generator of folder_id
        '''
        ann_id = self._annotator(ann_id)
        if len(self.prefix) > 0:
            prefix = '|'.join([urllib.quote(self.prefix, safe='~'),
                               'topic', ann_id, ''])
        else:
            prefix = '|'.join(['topic', ann_id, ''])
        logger.info('Scanning for folders with prefix %r', prefix)
        return imap(lambda id: self.unwrap_folder_content_id(id)['folder_id'],
                    self.store.scan_prefix_ids(prefix))

    def subfolders(self, folder_id, ann_id=None):
        '''Yields an unodered generator of subfolders in a folder.

        By default (with ``ann_id=None``), subfolders are shown for all
        anonymous users. Optionally, ``ann_id`` can be set to a username,
        which restricts the list to only subfolders owned by that user.

        :param str folder_id: Folder id
        :param str ann_id: Username
        :rtype: generator of subfolder_id
        '''
        self.assert_valid_folder_id(folder_id)
        ann_id = self._annotator(ann_id)
        folder_cid = self.wrap_folder_content_id(ann_id, folder_id)
        if self.store.get(folder_cid) is None:
            raise KeyError(folder_id)
        all_labels = self.label_store.directly_connected(folder_cid)
        return nub(la.subtopic_for(folder_cid) for la in all_labels)

    def parent_subfolders(self, ident, ann_id=None):
        '''An unordered generator of parent subfolders for ``ident``.

        ``ident`` can either be a ``content_id`` or a tuple of
        ``(content_id, subtopic_id)``.

        Parent subfolders are limited to the annotator id given.

        :param ident: identifier
        :type ident: ``str`` or ``(str, str)``
        :param str ann_id: Username
        :rtype: generator of ``(folder_id, subfolder_id)``
        '''
        ann_id = self._annotator(ann_id)
        cid, _ = normalize_ident(ident)
        for lab in self.label_store.directly_connected(ident):
            folder_cid = lab.other(cid)
            subfolder_sid = lab.subtopic_for(folder_cid)
            if not folder_cid.startswith('topic|'):
                continue
            folder = self.unwrap_folder_content_id(folder_cid)
            subfolder = self.unwrap_subfolder_subtopic_id(subfolder_sid)
            if folder['annotator_id'] != ann_id:
                continue
            yield (folder['folder_id'], subfolder)

    def items(self, folder_id, subfolder_id, ann_id=None):
        '''Yields an unodered generator of items in a subfolder.

        The generator yields items, which are represented by a tuple
        of ``content_id`` and ``subtopic_id``. The format of these
        identifiers is unspecified.

        By default (with ``ann_id=None``), subfolders are shown for all
        anonymous users. Optionally, ``ann_id`` can be set to a username,
        which restricts the list to only subfolders owned by that user.

        :param str folder_id: Folder id
        :param str subfolder_id: Subfolder id
        :param str ann_id: Username
        :rtype: generator of ``(content_id, subtopic_id)``
        '''
        self.assert_valid_folder_id(folder_id)
        self.assert_valid_folder_id(subfolder_id)
        ann_id = self._annotator(ann_id)
        folder_cid = self.wrap_folder_content_id(ann_id, folder_id)
        subfolder_sid = self.wrap_subfolder_subtopic_id(subfolder_id)
        ident = (folder_cid, subfolder_sid)

        if self.store.get(folder_cid) is None:
            raise KeyError(folder_id)
        for lab in self.label_store.directly_connected(ident):
            cid = lab.other(folder_cid)
            subid = lab.subtopic_for(cid)
            yield (cid, subid)

    def grouped_items(self, folder_id, subfolder_id, ann_id=None):
        '''Returns a dictionary from content ids to subtopic ids.

        Namely, the mapping is ``content_id |--> list of subtopic id``.

        By default (with ``ann_id=None``), subfolders are shown for all
        anonymous users. Optionally, ``ann_id`` can be set to a username,
        which restricts the list to only subfolders owned by that user.

        :param str folder_id: Folder id
        :param str subfolder_id: Subfolder id
        :param str ann_id: Username
        :rtype: ``dict`` of ``content_id |--> [subtopic_id]``
        '''
        d = defaultdict(list)
        for cid, subid in self.items(folder_id, subfolder_id, ann_id=ann_id):
            d[cid].append(subid)
        return d

    def add_folder(self, folder_id, ann_id=None):
        '''Add a folder.

        If ``ann_id`` is set, then the folder is owned by the given user.
        Otherwise, the folder is owned and viewable by all anonymous
        users.

        :param str folder_id: Folder id
        :param str ann_id: Username
        '''
        self.assert_valid_folder_id(folder_id)
        ann_id = self._annotator(ann_id)
        cid = self.wrap_folder_content_id(ann_id, folder_id)
        self.store.put([(cid, FeatureCollection())])
        logger.info('Added folder %r with content id %r', folder_id, cid)

    def add_item(self, folder_id, subfolder_id, content_id, subtopic_id=None,
                 ann_id=None):
        '''Add an item to a subfolder.

        The format of ``content_id`` and ``subtopic_id`` is
        unspecified. It is application specific.

        If ``ann_id`` is set, then the item is owned by the given user.
        Otherwise, the item is owned and viewable by all anonymous
        users.

        :param str folder_id: Folder id
        :param str subfolder_id: Folder id
        :param str content_id: content identifier
        :param str subtopic_id: subtopic identifier
        :param str ann_id: Username
        '''
        self.assert_valid_folder_id(folder_id)
        self.assert_valid_folder_id(subfolder_id)
        ann_id = self._annotator(ann_id)
        folder_cid = self.wrap_folder_content_id(ann_id, folder_id)
        subfolder_sid = self.wrap_subfolder_subtopic_id(subfolder_id)

        if self.store.get(folder_cid) is None:
            raise KeyError(folder_id)

        lab = Label(folder_cid, content_id,
                    ann_id, CorefValue.Positive,
                    subtopic_id1=subfolder_sid,
                    subtopic_id2=subtopic_id)
        self.label_store.put(lab)
        logger.info('Added subfolder item: %r', lab)

    def _annotator(self, ann_id):
        '''Returns the default annotator iff ``ann_id is None``.'''
        return self.DEFAULT_ANNOTATOR_ID if ann_id is None else ann_id

    def assert_valid_folder_id(self, ident):
        if ' ' in ident or '/' in ident:
            raise ValueError("Folder ids cannot contain spaces "
                             "or '/' characters.")

    def wrap_folder_content_id(self, annotator_id, fid):
        prefix = urllib.quote(self.prefix, safe='~')
        parts = [prefix] if len(prefix) > 0 else []
        parts.extend([
            'topic',
            urllib.quote(annotator_id, safe='~'),
            urllib.quote(fid, safe='~'),
        ])
        return '|'.join(parts)

    def unwrap_folder_content_id(self, cid):
        parts = cid.split('|')
        if len(parts) == 3:
            _, annotator_id, fid = parts
        else:
            _, _, annotator_id, fid = parts
        return {
            'annotator_id': urllib.unquote(annotator_id),
            'folder_id': urllib.unquote(fid),
        }

    def wrap_subfolder_subtopic_id(self, sfid):
        return sfid

    def unwrap_subfolder_subtopic_id(self, subtopic_id):
        return subtopic_id


def nub(it):
    '''Dedups an iterable in arbitrary order.

    Uses memory proportional to the number of unique items in ``it``.
    '''
    seen = set()
    for v in it:
        h = hash(v)
        if h in seen:
            continue
        seen.add(h)
        yield v


def dedup(it):
    '''Dedups a sorted iterable in constant memory.'''
    for _, group in groupby(it):
        for lab in group:
            yield lab
            break


def normalize_ident(ident):
    '''Splits a generic identifier.

    If ``ident`` is a tuple, then ``(ident[0], ident[1])`` is returned.
    Otherwise, ``(ident[0], None)`` is returned.
    '''
    if isinstance(ident, tuple) and len(ident) == 2:
        return ident[0], ident[1]  # content_id, subtopic_id
    else:
        return ident, None
