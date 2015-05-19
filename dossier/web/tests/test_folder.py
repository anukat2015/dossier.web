from __future__ import absolute_import, division, print_function

import pytest

from dossier.web.folder import \
    Folders, InodeCreationError, NotDirectoryError, \
    RootDeleteError, RootMoveError, \
    PathExistsError, StemNotItemError
from dossier.web.tests import kvl


@pytest.yield_fixture
def folders(kvl):
    yield Folders(kvl)


def test_folder_put_get(folders):
    folders.put('/a/b/c', 'foo')
    assert folders.get('/a/b/c').data == 'foo'


def test_root(folders):
    folders.put('/a', 'foo')
    assert folders.get('a').data == 'foo'


def test_not_directory(folders):
    folders.put('/a', 'foo')
    with pytest.raises(NotDirectoryError):
        folders.put_folder('/a/b')


def test_stem_not_directory(folders):
    folders.put('/a', 'foo')
    with pytest.raises(NotDirectoryError):
        folders.put_folder('/a')


def test_stem_not_item(folders):
    folders.put_folder('/a')
    with pytest.raises(StemNotItemError):
        folders.put('/a', 'foo')


def test_get_not_directory(folders):
    folders.put('/a/b/c', 'foo')
    with pytest.raises(NotDirectoryError):
        folders.get('/a/b/c/d')


def test_get_key_error(folders):
    with pytest.raises(KeyError):
        folders.get('does not exist')


def test_folder_no_data(folders):
    with pytest.raises(AssertionError):
        folders.put('a', data='foo', is_folder=True)


def test_item_no_data(folders):
    folders.put('a')
    assert folders.get('a').data == ''


def test_item_data_invalid_type(folders):
    with pytest.raises(AssertionError):
        folders.put('a', u'foo')


def test_normal_paths(folders):
    folders.put('//a////b/c', 'z')
    assert folders.get('a/b/c').data == 'z'
    assert folders.get('/a/b/c').data == 'z'
    assert folders.get('//a//b//c').data == 'z'
    assert folders.get('a/b///////////////////c').data == 'z'


def test_parent_root(folders):
    assert folders.root() == folders.parent('')
    assert folders.root() == folders.parent('/')
    assert folders.root() == folders.parent('//')


def test_parent(folders):
    folders.put('/a/b/c')
    assert folders.parent('/a/b/c') == folders.get('/a/b')
    assert folders.parent('/a/b') == folders.get('/a')
    assert folders.parent('/a') == folders.get('/')


def test_delete_root(folders):
    with pytest.raises(RootDeleteError):
        folders.delete('/')


def test_delete_item(folders):
    folders.put('/a/b')
    folders.get('/a/b')
    folders.delete('/a/b')
    with pytest.raises(KeyError):
        folders.get('/a/b')


def test_delete_folder(folders):
    folders.put_folder('/a')
    folders.put('/a/b')
    folders.get('/a')
    folders.get('/a/b')
    folders.delete('/a')
    with pytest.raises(KeyError):
        folders.get('/a')
    with pytest.raises(KeyError):
        folders.get('/a/b')


def test_delete_everything(folders):
    folders.put('/a/b')
    folders.put('/z')
    assert len(list(folders.list('/'))) == 2
    folders.delete_all()
    assert len(list(folders.list('/'))) == 0


def test_idempotent_put(folders):
    folders.put('/a/b/c', 'foo')
    folders.put_folder('/a/b')
    assert folders.get('/a/b/c').data == 'foo'


def test_put_overwrite(folders):
    folders.put('/a/b/c', 'foo')
    assert folders.get('/a/b/c').data == 'foo'
    folders.put('/a/b/c', 'bar')
    assert folders.get('/a/b/c').data == 'bar'


def test_move_root(folders):
    with pytest.raises(RootMoveError):
        folders.move('/', '/a')


def test_move_exists(folders):
    folders.put('/a')
    folders.put('/b')
    with pytest.raises(PathExistsError):
        folders.move('/a', '/b')


def test_move_item(folders):
    folders.put('/a', 'foo')
    folders.move('/a', '/b')
    assert folders.get('/b').data == 'foo'
    with pytest.raises(KeyError):
        folders.get('/a')


def test_move_folder(folders):
    folders.put('/a/b', 'foo')
    folders.move('/a', '/b')
    assert folders.get('/b/b').data == 'foo'
    with pytest.raises(KeyError):
        folders.get('/a')
    with pytest.raises(KeyError):
        folders.get('/a/b')


def test_move_and_create(folders):
    folders.put('/a', 'foo')
    folders.move('/a', '/x/y/z/a')
    assert folders.get('/x/y/z/a').data == 'foo'


def test_list(folders):
    folders.put('/a/b/c')
    folders.put_folder('/a/b/d')
    folders.put('/a/b/e')
    assert list(folders.list('/a/b')) == [
        folders.get('/a/b/c'),
        folders.get('/a/b/d'),
        folders.get('/a/b/e'),
    ]
    assert list(folders.list('/a/b', recursive=True)) == [
        folders.get('/a/b/c'),
        folders.get('/a/b/d'),
        folders.get('/a/b/e'),
    ]


def test_list_recursive(folders):
    folders.put('/a/d/g')
    folders.put('/a/b')
    folders.put_folder('/a/c')
    folders.put('/a/d/e/f')
    assert list(folders.list('/', recursive=True)) == [
        folders.get('/a'),
        folders.get('/a/b'),
        folders.get('/a/c'),
        folders.get('/a/d'),
        folders.get('/a/d/e'),
        folders.get('/a/d/e/f'),
        folders.get('/a/d/g'),
    ]


def test_failed_inode_creation(folders):
    # This test is suspicious. It should cause some failures if tests are
    # run simultaneously, but I can't seem to provoke it. xfail if it gives
    # you trouble. ---AG
    import time
    old_time = time.time
    time.time = lambda: 1  # this is why parallel tests might fail
    folders.put('a')  # This succeeds because it's the first inode
    with pytest.raises(InodeCreationError):
        folders.put('b')  # fails because it always tries to use inode `1`
    time.time = old_time
