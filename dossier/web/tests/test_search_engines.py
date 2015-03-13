from __future__ import absolute_import, division, print_function

from dossier.fc import FeatureCollection
import dossier.web.search_engines as search_engines
from dossier.web.tests import kvl, store

def test_random_no_name_index(store):
    assert u'NAME' not in store.index_names()
    store.put([('foo', FeatureCollection({u'NAME': {'bar': 1}}))])
    random = search_engines.random(store)
    random('foo', lambda _: True, 100)  # just make sure it runs
