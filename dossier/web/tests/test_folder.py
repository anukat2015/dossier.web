from __future__ import absolute_import, division, print_function

import pytest

from dossier.web.folder import Folders
from dossier.web.tests import kvl, store, label_store


@pytest.yield_fixture
def folders(store, label_store):
    yield Folders(store, label_store)


def test_folder_add(folders):
    folders.add_folder('foo_bar')
    assert list(folders.folders()) == ['foo_bar']


def test_folder_add_annotator(folders):
    folders.add_folder('foo', ann_id='ann_foo')
    folders.add_folder('bar', ann_id='ann_bar')
    assert list(folders.folders()) == []
    assert list(folders.folders(ann_id='ann_foo')) == ['foo']
    assert list(folders.folders(ann_id='ann_bar')) == ['bar']


def test_folder_add_bad_id(folders):
    with pytest.raises(ValueError):
        folders.add_folder('foo bar')
    with pytest.raises(ValueError):
        folders.add_folder('foo/bar')


def test_subfolder_add(folders):
    folders.add_folder('foo')
    folders.add_item('foo', 'subfoo', 'a', 'suba')
    assert list(folders.subfolders('foo')) == ['subfoo']
    assert list(folders.items('foo', 'subfoo')) == [('a', 'suba')]


def test_subfolder_add_annotator(folders):
    folders.add_folder('foo', ann_id='ann_foo')
    folders.add_folder('bar', ann_id='ann_bar')
    folders.add_item('foo', 'subfoo', 'a', 'suba', ann_id='ann_foo')
    folders.add_item('bar', 'subbar', 'b', 'subb', ann_id='ann_bar')

    # Make sure the default annotator doesn't see anything.
    with pytest.raises(KeyError):
        folders.subfolders('foo')
    with pytest.raises(KeyError):
        folders.subfolders('bar')
    with pytest.raises(KeyError):
        next(folders.items('foo', 'subfoo'))
    with pytest.raises(KeyError):
        next(folders.items('bar', 'subbar'))

    assert list(folders.subfolders('foo', ann_id='ann_foo')) == ['subfoo']
    assert list(folders.subfolders('bar', ann_id='ann_bar')) == ['subbar']
    assert list(folders.items('foo', 'subfoo', ann_id='ann_foo')) \
        == [('a', 'suba')]
    assert list(folders.items('bar', 'subbar', ann_id='ann_bar')) \
        == [('b', 'subb')]


def test_subfolder_add_bad_id(folders):
    folders.add_folder('foo')
    with pytest.raises(ValueError):
        folders.add_item('foo', 'sub foo', 'a', 'suba')
    with pytest.raises(ValueError):
        folders.add_item('foo', 'sub/foo', 'a', 'suba')
