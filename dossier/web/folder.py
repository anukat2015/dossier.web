from __future__ import absolute_import, division, print_function

from collections import namedtuple
import re

import cbor


class NotDirectory(Exception):
    def __init__(self, path, part_of=None):
        self.path = path
        self.part_of = part_of


class Item(namedtuple('Item', 'namespace owner inode name meta_data data')):
    def is_folder(self):
        return False


class Folder(Item):
    def is_folder(self):
        return True


class Folders(object):
    config_name = 'dossier.folder'
    TABLE = 'folders'
    DEFAULT_OWNER_ID = 'unknown'

    _kvlayer_namespace = {
        # (namespace, owner, inode) -> data
        # N.B. Root node is always at inode `0`.
        TABLE: (str, str, long),
    }

    def __init__(self, kvl, namespace=''):
        self.kvl = kvl
        self.namespace = utf8(namespace)

    def key(self, inode, owner=None):
        return (self.namespace, utf8(owner or self.DEFAULT_OWNER_ID), inode)

    def from_inode(self, name, inode, owner=None):
        key = self.key(inode, owner=owner)
        data = self.kvl.get(key)
        if data is None:
            raise KeyError(inode)
        return Folders.kvlayer_to_folder(name, key, data)

    def get(self, path, owner=None):
        path = normalize_path(path)
        cur = self.from_inode(u'/', 0, owner=owner)
        components = path.split('/')
        for i, component in enumerate(components):
            if not cur.is_folder():
                # This is the user trying to use a non-folder in a non-leaf
                # position.
                raise NotDirectory('/' + path_join(components[0:i+1]), path)
            try:
                inode = cur.child(component)
            except KeyError:
                raise KeyError(path)
            try:
                cur = self.from_inode(component, inode, owner=owner)
            except KeyError:
                raise KeyError(path)

    def put(self, path, data):
        pass

    @staticmethod
    def kvlayer_to_folder(name, key, data):
        return Folders.kvlayer_to_item(name, key, data, factory=Folder)

    @staticmethod
    def kvlayer_to_item(name, key, data, factory=Item):
        data = cbor.loads(data)
        meta_data = data['meta_data']
        user_data = data['user_data']
        return Folder(namespace=uni(key[0]), owner=uni(key[1]), inode=key[2],
                      name=uni(name), meta_data=meta_data, data=user_data)

    @staticmethod
    def kvlayer_from_item(item):
        data = cbor.dumps({
            'meta_data': item.meta_data,
            'user_data': item.user_data,
        })
        return ((utf8(item.namespace), utf8(item.owner), item.inode), data)


def normalize_path(path):
    path = re.sub('/+', '/', uni(path))
    if path.startswith('/'):
        path = path[1:]
    return path


def path_join(components):
    return '/'.join(components)


def uni(s):
    if not isinstance(s, unicode):
        return s.decode('utf-8')
    return s


def utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    return s
