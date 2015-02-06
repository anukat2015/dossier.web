from __future__ import absolute_import, division, print_function

import json
from cStringIO import StringIO
import urllib

import bottle

from dossier.fc import FeatureCollection
import dossier.web.routes as routes
from dossier.web.tests import kvl, store, label_store


def rot14(s):
    # Use `rot14` so that `rot14(rot14(s)) != s`.
    return ''.join(chr(ord('a') + ((ord(c) - ord('a')) + 14) % 26) for c in s)


def dbid_to_visid(s):
    return rot14(s)


def visid_to_dbid(s):
    return rot14(s)


def new_request(params=None, body=None):
    environ = {}
    if params is not None:
        environ['QUERY_STRING'] = urllib.urlencode(params)
    if body is not None:
        environ['wsgi.input'] = StringIO(body)
        environ['CONTENT_LENGTH'] = len(body)
    return bottle.Request(environ=environ)


def new_response():
    return bottle.Response()


def test_fc_put(store):
    req = new_request(body=json.dumps({'foo': {'a': 1}}))
    resp = new_response()
    routes.v1_fc_put(req, resp, visid_to_dbid, store, 'abc')

    assert store.get(visid_to_dbid('abc'))['foo']['a'] == 1


def test_fc_get(store):
    store.put([(visid_to_dbid('abc'), FeatureCollection({'foo': {'a': 1}}))])
    fc = routes.v1_fc_get(dbid_to_visid, store, 'abc')
    assert fc['foo']['a'] == 1
