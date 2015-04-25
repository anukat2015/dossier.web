'''Tests for dossier.web.filter_preds filtering functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
import pytest

from dossier.fc import FeatureCollection, StringCounter
from nilsimsa import Nilsimsa

from dossier.web.tests import kvl, store, label_store
from dossier.web.filter_preds import near_duplicates

def nilsimsa_hash(text):
    if isinstance(text, unicode):
        text = text.encode('utf8')
    return Nilsimsa(text).hexdigest()

near_duplicate_texts = [
    'The quick brown fox jumps over the lazy dog.',
    'The quick brown fox jumps over the lazy dogs.',
    'The quick brown foxes jumped over the lazy dog.',
    'The quick brown foxes jumped over the lazy dogs.',
    ]


def test_near_duplicates(label_store, store):

    def make_fc(text):
        nhash = nilsimsa_hash(text)
        fc = FeatureCollection()
        fc['#nilsimsa_all'] = StringCounter([nhash])
        return fc

    fcs = [(str(idx), make_fc(text)) 
           for idx, text in enumerate(near_duplicate_texts)]
    query_content_id, query_fc = fcs.pop(0)

    store.put([(query_content_id, query_fc)])

    init_filter = near_duplicates(
        label_store, store, 
        ## lower threshold for short test strings
        threshold=0.6)

    accumulating_predicate = init_filter(query_content_id)
    
    results = filter(accumulating_predicate, fcs)
    assert len(results) == 0
