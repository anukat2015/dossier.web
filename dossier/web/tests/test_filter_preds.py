'''Tests for dossier.web.filter_preds filtering functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from itertools import chain, repeat
import pytest
import random
import string
import time

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


def make_fc(text):
    nhash = nilsimsa_hash(text)
    fc = FeatureCollection()
    fc['#nilsimsa_all'] = StringCounter([nhash])
    return fc


def test_near_duplicates_basic(label_store, store):

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


def test_near_duplicates_update_logic(label_store, store):

    fcs = [(str(idx), make_fc(text))
           for idx, text in enumerate(chain(*repeat(near_duplicate_texts, 1000)))]

    query_content_id, query_fc = fcs.pop(0)

    store.put([(query_content_id, query_fc)])

    init_filter = near_duplicates(
        label_store, store,
        ## lower threshold for short test strings
        threshold=0.95)

    accumulating_predicate = init_filter(query_content_id)

    start = time.time()
    results = filter(accumulating_predicate, fcs)
    elapsed = time.time() - start
    print '%d filtered to %d in %f seconds, %f per second' % (
        len(fcs), len(results), elapsed, len(fcs) / elapsed)

    assert len(results) == 3


def random_text(N=3500):
    '''generate a random text of length N
    '''
    candidates = string.ascii_lowercase + string.ascii_uppercase + string.digits 
    ## make whitespaces appear approx 1/7 times
    candidates += ' ' * (len(candidates) / 7)
    return ''.join(random.choice(candidates) for _ in range(N))


def test_near_duplicates_speed_perf(label_store, store, num_texts=5, num_dups_each=10):

    different_texts = [random_text() for _ in range(num_texts)]

    fcs = [(str(idx), make_fc(text))
           for idx, text in enumerate(chain(*repeat(different_texts, num_dups_each)))]

    query_content_id, query_fc = fcs.pop(0)

    store.put([(query_content_id, query_fc)])

    init_filter = near_duplicates(
        label_store, store,
        ## lower threshold for short test strings
        threshold=0.95)

    accumulating_predicate = init_filter(query_content_id)

    start = time.time()
    results = filter(accumulating_predicate, fcs)
    elapsed = time.time() - start
    print '%d filtered to %d in %f seconds, %f per second' % (
        len(fcs), len(results), elapsed, len(fcs) / elapsed)

    assert len(results) == num_texts - 1 # minus the query

# dossier/web/tests/test_filter_preds.py::test_near_duplicates_speed_perf 4999 filtered to 49 in 2.838213 seconds, 1761.319555 per second
