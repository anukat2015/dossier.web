from __future__ import absolute_import, division, print_function

from collections import namedtuple
from operator import itemgetter
import random
import re
import time

import cbor


class Item(namedtuple('Item', 'namespace owner inode path meta_data data')):
    def is_folder(self):
        return False

    def is_item(self):
        return True

    @property
    def name(self):
        if self.path is None:
            return None
        return path_base(self.path)

    def __eq__(self, other):
        return self.inode == other.inode


class Folder(Item):
    def __init__(self, *args, **kwargs):
        super(Folder, self).__init__(*args, **kwargs)

    def is_folder(self):
        return True

    def is_item(self):
        return False

    def inodes(self):
        return self.meta_data.get('children', {}).iteritems()

    def get_inode(self, name):
        return self.meta_data.get('children', {})[uni(name)]

    def add_inode(self, name, inode):
        self.meta_data.setdefault('children', {})[uni(name)] = inode

    def delete_inode(self, name):
        self.meta_data.setdefault('children', {}).pop(uni(name))


class Folders(object):
    config_name = 'dossier.folders'
    TABLE = 'folders'

    _kvlayer_namespace = {
        # (namespace, owner, inode) -> data
        # N.B. Root node is always at inode `0`.
        TABLE: (str, str, long),
    }

    def __init__(self, kvl, namespace='', owner='unknown'):
        self.kvl = kvl
        self.kvl.setup_namespace(self._kvlayer_namespace)
        self.namespace = utf8(namespace)
        self.owner = utf8(owner)

    def key(self, inode):
        return (self.namespace, self.owner, long(inode))

    def kvl_get(self, key):
        for _, v in self.kvl.get(self.TABLE, key):
            if v is not None:
                return v
        return None

    def from_inode(self, inode, path=None):
        key = self.key(inode)
        data = self.kvl_get(key)
        if data is None:
            raise KeyError(inode)
        return Folders.from_kvlayer(key, data, path=path)

    def from_inode_must(self, inode, path=None):
        try:
            return self.from_inode(inode, path=path)
        except KeyError:
            # This is actually a bug! If there's an inode in a folder,
            # then that inode better exist in the table.
            # Well... It should be a bug. But this is written to not care about
            # data races, which means an inode could disappear.
            raise InodeDisappearedError(inode)

    def root(self):
        try:
            return self.from_inode(0, path=u'/')
        except KeyError:
            root = Folder(namespace=self.namespace, owner=self.owner,
                          inode=0L, path=u'/', meta_data={}, data='')
            self.kvl.put(self.TABLE, Folders.to_kvlayer(root))
            return root

    def parent(self, path):
        item = self.get(path_folder(path))
        assert item.is_folder()
        return item

    def get(self, path):
        cur = self.root()
        for _, prev, component in path_components(path):
            if cur.is_item():
                # This is the user trying to use a non-folder in a non-leaf
                # position.
                raise NotDirectoryError(prev + '/' + component, path)
            try:
                inode = cur.get_inode(component)
            except KeyError:
                raise KeyError(path)
            cur = self.from_inode_must(inode, path=canonical_path(path))
        return cur

    def put_folder(self, path):
        self.put(path, data='', is_folder=True)

    def put(self, path, data='', is_folder=False):
        assert isinstance(data, str)
        assert not is_folder or len(data) == 0

        cur = self.root()
        for is_stem, prev, component in path_components(path):
            if cur.is_item():
                raise NotDirectoryError(prev + '/' + component, path)
            try:
                inode = cur.get_inode(component)
                child = self.from_inode_must(inode)
                if is_stem:
                    if is_folder and child.is_item():
                        raise NotDirectoryError(prev + '/' + component, path)
                    elif not is_folder and child.is_folder():
                        raise StemNotItemError(prev + '/' + component, path)
                    if child.is_item():
                        self.kvl.put(
                            self.TABLE,
                            Folders.to_kvlayer(child._replace(data=data)))
            except KeyError:
                factory = Item if is_stem and not is_folder else Folder
                child = factory(namespace=self.namespace, owner=self.owner,
                                inode=self.fresh_inode(), path=None,
                                meta_data={}, data=data)
                self.kvl.put(self.TABLE, Folders.to_kvlayer(child))
                cur.add_inode(component, child.inode)
                self.kvl.put(self.TABLE, Folders.to_kvlayer(cur))
            cur = child

    def delete(self, path):
        # We don't actually delete the inodes.
        # We certainly could do that here, and frankly, it wouldn't be too
        # big of a deal. But, it seems like good juju to not truly delete
        # folders.
        # Instead, we simply detach the given path from its parent.
        # This creates a unreachable dangling inode.
        folder, base = path_folder(path), path_base(path)
        if base is None:
            raise RootDeleteError
        parent = self.get(folder)
        assert parent == self.parent(path)
        try:
            parent.delete_inode(base)
        except KeyError:
            raise KeyError(path)
        self.kvl.put(self.TABLE, Folders.to_kvlayer(parent))

    def delete_all(self):
        for it in self.list('/'):
            self.delete(it.path)

    def move(self, src_path, dest_path):
        src = self.get(src_path)
        if src.inode == 0:
            raise RootMoveError
        try:
            self.get(dest_path)
            raise PathExistsError(dest_path)
        except KeyError:
            pass
        # This is a little circuitous, but no need to optimize round trips
        # yet. ---AG
        self.delete(src_path)
        dest_folder, dest_base = path_folder(dest_path), path_base(dest_path)
        self.put_folder(dest_folder)  # idempotent
        dest_item = self.get(dest_folder)
        dest_item.add_inode(dest_base, src.inode)
        self.kvl.put(self.TABLE, Folders.to_kvlayer(dest_item))

    def list(self, path, recursive=False):
        def child_items(f):
            for name, inode in sorted(f.inodes(), key=itemgetter(0)):
                path = canonical_path(f.path + '/' + name)
                yield self.from_inode_must(inode, path=path)

        item = self.get(path)
        if not item.is_folder():
            return
        if not recursive:
            for citem in child_items(item):
                yield citem
            return

        # Use an explicit stack because Guido hates functional programming.
        # Allocations everywhere!
        stack = list(reversed(list(child_items(item))))
        while len(stack) > 0:
            item = stack.pop()
            yield item
            if item.is_folder():
                stack.extend(reversed(list(child_items(item))))

    def fresh_inode(self):
        start = lambda: long(time.time() * 1000)
        for _ in xrange(10):
            inode = start()
            key = self.key(inode)
            if self.kvl_get(key) is None:
                return inode
            time.sleep(float(random.randrange(10, 50)) / 1000.0)
        raise InodeCreationError

    @staticmethod
    def from_kvlayer(key, data, path=None):
        data = cbor.loads(data)
        meta_data = data['meta_data']
        user_data = data['user_data']
        factory = Item
        if data.get('is_folder', False):
            factory = Folder
        return factory(namespace=uni(key[0]), owner=uni(key[1]), inode=key[2],
                       path=path, meta_data=meta_data, data=user_data)

    @staticmethod
    def to_kvlayer(item):
        data = cbor.dumps({
            'is_folder': item.is_folder(),
            'meta_data': item.meta_data,
            'user_data': item.data,
        })
        return ((utf8(item.namespace), utf8(item.owner), item.inode), data)


class NotDirectoryError(Exception):
    def __init__(self, path, part_of=None):
        self.path = path
        self.part_of = part_of


class StemNotItemError(Exception):
    def __init__(self, path, part_of=None):
        self.path = path
        self.part_of = part_of


class PathExistsError(Exception):
    def __init__(self, path):
        self.path = path


class InodeCreationError(Exception):
    pass


class InodeDisappearedError(Exception):
    pass


class RootDeleteError(Exception):
    pass


class RootMoveError(Exception):
    pass


def path_components(path):
    path = normalize_path(path)
    components = filter(None, path.split('/'))
    for i, component in enumerate(components):
        prev = '/' + path_join(components[0:i])
        yield i+1 == len(components), prev, component


def path_folder(path):
    return '/' + '/'.join(map(itemgetter(2), path_components(path))[:-1])


def path_base(path):
    base = None
    for _, _, component in path_components(path):
        base = component
    return base


def canonical_path(path):
    return '/' + normalize_path(path)


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
