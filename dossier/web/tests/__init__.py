from __future__ import absolute_import, division, print_function

import pytest

from dossier.label import LabelStore
from dossier.store import Store
import kvlayer
import yakonfig

@pytest.yield_fixture
def kvl():
    config = {
        'storage_type': 'local',
        'app_name': 'diffeo',
        'namespace': 'dossier.web.tests',
    }
    with yakonfig.defaulted_config([kvlayer], params=config) as config:
        client = kvlayer.client()
        yield client
        client.delete_namespace()
        client.close()


@pytest.yield_fixture
def store(kvl):
    yield Store(kvl, feature_indexes=['foo', 'bar'])


@pytest.yield_fixture
def label_store(kvl):
    yield LabelStore(kvl)
