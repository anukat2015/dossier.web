from __future__ import absolute_import, division, print_function

import json
import time
import urllib

from dossier.web.tags import TagsSync
import dossier.web.tags as tag_routes
from dossier.web.tests.test_routes import new_request
import pytest


NAMESPACE = 'dossier_web_tests'
DUMMY_ASSOC = {
    u'url': u'http://foo.com/abc/hello world/?foo=bar&baz=foo',
    u'text': u'Foo to the Bar to the Baz.',
    u'stream_id': u'123456789-somemd5hash',
    u'hash': u'nilsimsa-hash',
    u'timestamp': int(time.time()),
    u'xpath': {
        u'start_node': u'/html/body/p[1]',
        u'start_idx': 0,
        u'end_node': u'/html/body/p[2]',
        u'end_idx': 5,
    }
}


@pytest.yield_fixture  # noqa
def tags(elastic_address, namespace_string):
    x = TagsSync(hosts=elastic_address, namespace=NAMESPACE,
                 type_prefix=namespace_string)
    yield x
    x.delete_all()


def test_tag_associate(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    assert tags.list(u'') == [{
        'tag': 'foo', 'name': 'foo', 'parent': '',
    }]
    assert tags.list(u'foo') == [{
        'tag': 'foo/bar', 'name': 'bar', 'parent': 'foo',
    }]
    assert tags.list(u'foo/bar') == [{
        'tag': 'foo/bar/baz', 'name': 'baz', 'parent': 'foo/bar',
    }]
    tagged = dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})
    assert tags.assocs_by_tag(u'foo/bar/baz') == [tagged]


def test_tag_associate_twice(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    assert tags.list(u'') == [{
        'tag': 'foo', 'name': 'foo', 'parent': '',
    }]
    assert tags.list(u'foo') == [{
        'tag': 'foo/bar', 'name': 'bar', 'parent': 'foo',
    }]
    assert tags.list(u'foo/bar') == [{
        'tag': 'foo/bar/baz', 'name': 'baz', 'parent': 'foo/bar',
    }]
    tagged = dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})
    assert tags.assocs_by_tag(u'foo/bar/baz') == [tagged, tagged]


def test_tag_associate_no_validate(tags):
    assoc = {
        'url': 'http://foo.com',
        'text': 'Foo to the Bar to the Baz.',
        'stream_id': '123456789-somemd5hash',
        'hash': 'nilsimsa-hash',
        'timestamp': int(time.time()),
    }
    req = new_request(body=json.dumps(assoc))
    with pytest.raises(ValueError):
        tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')


def test_tag_list(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    assert tag_routes.v1_tag_list(tags, '') == {
        'children': [{u'name': u'foo', u'parent': u'', u'tag': u'foo'}],
    }
    assert tag_routes.v1_tag_list(tags, 'foo') == {
        'children': [{u'name': u'bar', u'parent': u'foo', u'tag': u'foo/bar'}],
    }


def test_tag_associations(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    assert tag_routes.v1_tag_associations(tags, 'foo/bar/baz') == {
        'associations': [dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})],
    }


def test_url_associations(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    url = DUMMY_ASSOC[u'url']
    assert tag_routes.v1_url_associations(tags, url) == {
        'associations': [dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})],
    }
    assert tag_routes.v1_url_associations(tags, urllib.quote(url)) == {
        'associations': [dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})],
    }


def test_stream_id_associations(tags):
    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    sid = u'123456789-somemd5hash'
    assert tag_routes.v1_stream_id_associations(tags, sid) == {
        'associations': [dict(DUMMY_ASSOC, **{u'tag': u'foo/bar/baz'})],
    }


def test_suggest(tags):
    def get_suggest(parent, prefix, limit=100):
        req = new_request(params={'limit': limit})
        hits = tag_routes.v1_tag_suggest(req, tags, prefix, parent)
        return sorted(hits['suggestions'])

    req = new_request(body=json.dumps(DUMMY_ASSOC))
    tag_routes.v1_tag_associate(req, tags, 'foo/bar/baz')
    tag_routes.v1_tag_associate(req, tags, 'fob/bar/abc')

    assert get_suggest(u'', u'') == []
    assert get_suggest(u'foo', u'') == []
    assert get_suggest(u'', u'f') == [u'fob', u'foo']
    assert get_suggest(u'', u'fo') == [u'fob', u'foo']
    assert get_suggest(u'', u'foo') == [u'foo']
    assert get_suggest(u'', u'fob') == [u'fob']
    assert get_suggest(u'foo', u'b') == [u'bar']
    assert get_suggest(u'fob', u'b') == [u'bar']
    assert get_suggest(u'foo/bar', u'b') == [u'baz']
    assert get_suggest(u'fob/bar', u'b') == []
    assert get_suggest(u'fob/bar', u'a') == [u'abc']
    assert get_suggest(u'', 'f', limit=1) in ([u'fob'], [u'foo'])
