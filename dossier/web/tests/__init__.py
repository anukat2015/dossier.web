from __future__ import absolute_import, division, print_function

import pytest

from dossier.label import LabelStore
from dossier.store import ElasticStoreSync
import kvlayer
import yakonfig


@pytest.yield_fixture
def config_local(elastic_address, namespace_string):
    config = {
        'kvlayer': {
            'storage_type': 'local',
            'app_name': 'diffeo',
            'namespace': 'dossier.web.tests',
        },
        'dossier.store': {
            'hosts': [elastic_address],
            'namespace': 'tests',
            'type': namespace_string,
            'shards': 1,
            'replicas': 0,
            'feature_indexes': [{
                'foo': {
                    'feature_names': ['foo'],
                    'es_index_type': 'string',
                },
            }, {
                'bar': {
                    'feature_names': ['bar'],
                    'es_index_type': 'string',
                },
            }],
        },
    }
    modules = [ElasticStoreSync, kvlayer]
    with yakonfig.defaulted_config(modules, config=config) as config:
        yield config


@pytest.yield_fixture
def kvl(config_local):
    client = kvlayer.client()
    yield client
    client.delete_namespace()
    client.close()


@pytest.yield_fixture
def store(kvl):
    yield ElasticStoreSync.configured()


@pytest.yield_fixture
def label_store(kvl):
    yield LabelStore(kvl)
