from __future__ import absolute_import, division, print_function

import pytest

from dossier.fc import FeatureCollection
from dossier.store import Store
from dossier.web.config import Config
import kvlayer
import yakonfig


@pytest.mark.xfail  # Foldering is no longer configured with `Config`. ---AG
def test_folder_prefix():
    config = {
        'dossier.folders': {
            'prefix': 'foo',
        },
        'kvlayer': {
            'storage_type': 'local',
            'app_name': 'folder_prefix',
            'namespace': 'a',
        },
    }
    with yakonfig.defaulted_config([kvlayer], config=config):
        conf = Config(config=config)
        assert conf.folders.prefix == 'foo'


def test_same_conns():
    config = {
        'dossier.label': {},
        'kvlayer': {
            'storage_type': 'local',
            'app_name': 'same_conns',
            'namespace': 'a',
        },
    }
    with yakonfig.defaulted_config([kvlayer], config=config):
        conf = Config(config=config)
        store = conf.store
        store_from_labels = Store(conf.label_store.kvl)

        assert id(store.kvl) == id(conf.label_store.kvl)

        fc = FeatureCollection({u'foo': u'bar'})
        store.put([('a', fc)])
        assert fc == store_from_labels.get('a')


def test_different_conns():
    config = {
        'dossier.label': {
            'kvlayer': {
                'storage_type': 'local',
                'app_name': 'different_conns',
                'namespace': 'dossier.label',
            },
        },
        'kvlayer': {
            'storage_type': 'local',
            'app_name': 'different_conns',
            'namespace': 'dossier.store',
        }
    }
    with yakonfig.defaulted_config([kvlayer], config=config):
        conf = Config(config=config)
        store = conf.store
        store_from_labels = Store(conf.label_store.kvl)

        assert id(store.kvl) != id(conf.label_store.kvl)

        fc = FeatureCollection({u'foo': u'bar'})
        store.put([('a', fc)])
        assert store_from_labels.get('a') is None
