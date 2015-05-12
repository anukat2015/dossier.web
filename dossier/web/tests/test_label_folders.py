from __future__ import absolute_import, division, print_function

import pytest

from dossier.web.label_folders import Folders
from dossier.web.tests import kvl, store, label_store


@pytest.yield_fixture
def folders(store, label_store):
    yield Folders(store, label_store)


@pytest.yield_fixture
def folders_prefix(store, label_store):
    yield Folders(store, label_store, prefix='foo')


def test_folder_add(folders):
    folders.add_folder('foo_bar')
    assert list(folders.folders()) == ['foo_bar']


def test_folder_add_prefix(folders_prefix):
    folders_prefix.add_folder('foo_bar')
    assert list(folders_prefix.folders()) == ['foo_bar']


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


def test_subfolder_add_prefix(folders_prefix):
    folders_prefix.add_folder('foo')
    folders_prefix.add_item('foo', 'subfoo', 'a', 'suba')
    assert list(folders_prefix.subfolders('foo')) == ['subfoo']
    assert list(folders_prefix.items('foo', 'subfoo')) == [('a', 'suba')]


def test_subfolder_add_no_subtopic(folders):
    folders.add_folder('foo')
    folders.add_item('foo', 'subfoo', 'a')
    assert list(folders.subfolders('foo')) == ['subfoo']
    assert list(folders.items('foo', 'subfoo')) == [('a', '')]


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


def test_subfolder_add_no_folder(folders):
    with pytest.raises(KeyError):
        folders.add_item('foo', 'subfoo', 'a', 'suba')


def test_subfolder_add_no_folder_annotator(folders):
    folders.add_folder('foo', ann_id='ann_foo')
    with pytest.raises(KeyError):
        folders.add_item('foo', 'subfoo', 'a', 'suba')


def test_subfolder_add_bad_id(folders):
    folders.add_folder('foo')
    with pytest.raises(ValueError):
        folders.add_item('foo', 'sub foo', 'a', 'suba')
    with pytest.raises(ValueError):
        folders.add_item('foo', 'sub/foo', 'a', 'suba')


def test_parent_subfolders(folders):
    folders.add_folder('foo')
    folders.add_item('foo', 'subfoo', 'a', 'suba')
    assert list(folders.parent_subfolders('a')) == [('foo', 'subfoo')]
    assert list(folders.parent_subfolders(('a', 'suba'))) \
        == [('foo', 'subfoo')]


def test_parent_subfolders_annotator(folders):
    folders.add_folder('foo', ann_id='ann_foo')
    folders.add_item('foo', 'subfoo', 'a', 'suba', ann_id='ann_foo')
    folders.add_folder('bar', ann_id='ann_bar')
    folders.add_item('bar', 'subbar', 'a', 'suba', ann_id='ann_bar')

    # Make sure anonymous can't see them.
    assert list(folders.parent_subfolders('a')) == []
    assert list(folders.parent_subfolders(('a', 'suba'))) == []

    assert list(folders.parent_subfolders('a', ann_id='ann_foo')) \
        == [('foo', 'subfoo')]
    assert list(folders.parent_subfolders(('a', 'suba'), ann_id='ann_bar')) \
        == [('bar', 'subbar')]
