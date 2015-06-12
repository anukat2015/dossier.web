from __future__ import absolute_import, division, print_function

from dossier.fc import FeatureCollection
import dossier.web.search_engines as search_engines
from dossier.web.tests import kvl, store  # noqa


def test_random_no_name_index(store):  # noqa
    assert u'NAME' not in store.index_names()
    store.put([('foo', FeatureCollection({u'NAME': {'bar': 1}}))])
    # just make sure it runs
    search_engines.random(store).set_query_id('foo').results()
