'''Tests for dossier.web.filter_preds filtering functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
import copy
from itertools import chain, repeat
import pytest
import random
import string
import time

from dossier.fc import FeatureCollection as FC
from dossier.fc import FeatureCollection, StringCounter, GeoCoords
from nilsimsa import Nilsimsa

from dossier.web.tests import kvl, store, label_store
from dossier.web.filters import nilsimsa_near_duplicates, geotime


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


candidate_chars = (
    string.ascii_lowercase + string.ascii_uppercase + string.digits
)
## make whitespaces appear approx 1/7 times
candidate_chars += ' ' * (len(candidate_chars) / 7)

def random_text(N=3500):
    '''generate a random text of length N
    '''
    return ''.join(random.choice(candidate_chars) for _ in range(N))


def mutate(text, N=1):
    '''randomly change N characters in text
    '''
    new_text = []
    prev = 0
    for idx in sorted(random.sample(range(len(text)), N)):
        new_text.append(text[prev:idx])
        new_text.append(random.choice(candidate_chars))
        prev = idx + 1
    new_text.append(text[prev:])
    return ''.join(new_text)


@pytest.mark.skipif('1') ## no need to run this
@pytest.mark.xfail
def test_nilsimsa_exact_match():
    '''check that even though Nilsimsa has 256 bits to play with, you can
    pretty easily discover non-idential texts that have identical
    nilsimsa hashes.

    '''
    text0 = random_text(10**5)
    for _ in range(100):
        text1 = mutate(text0, N=1)
        if text0 != text1:
            assert nilsimsa_hash(text0) != nilsimsa_hash(text1)


def test_nilsimsa_near_duplicates_basic(label_store, store):

    fcs = [(str(idx), make_fc(text))
           for idx, text in enumerate(near_duplicate_texts)]
    query_content_id, query_fc = fcs.pop(0)

    store.put([(query_content_id, query_fc)])

    accumulating_predicate = nilsimsa_near_duplicates(
        label_store, store,
        ## lower threshold for short test strings
        threshold=0).set_query_id(query_content_id).create_predicate()

    assert len(filter(accumulating_predicate, fcs)) == 0


def test_nilsimsa_near_duplicates_update_logic(label_store, store):
    fcs = [(str(idx), make_fc(text))
           for idx, text in enumerate(chain(*repeat(near_duplicate_texts,
                                                    1000)))]

    query_content_id, query_fc = fcs.pop(0)

    store.put([(query_content_id, query_fc)])

    accumulating_predicate = nilsimsa_near_duplicates(
        label_store, store,
        ## lower threshold for short test strings
        threshold=120).set_query_id(query_content_id).create_predicate()

    start = time.time()
    results = filter(accumulating_predicate, fcs)
    elapsed = time.time() - start
    print '%d filtered to %d in %f seconds, %f per second' % (
        len(fcs), len(results), elapsed, len(fcs) / elapsed)
    assert len(results) == 3


## speed perf numbers in nilsimsa_near_duplicates doc string come from
## hand editing the kwargs in this:
def test_nilsimsa_near_duplicates_speed_perf(label_store, store, num_texts=5,
                                             num_exact_dups_each=10,
                                             num_near_dups_each=10):
    different_texts = [random_text() for _ in range(num_texts)]

    fcs = []
    for idx1, text in enumerate(different_texts):
        fc = make_fc(text)
        fcs.append(('%d-original-exact' % idx1, fc))
        for idx2 in range(num_exact_dups_each):
            fcs.append(('%d-%d-exact' % (idx1, idx2), copy.deepcopy(fc)))
        for idx2 in range(num_near_dups_each):
            fcs.append(
                ('%d-%d-exact' % (idx1, idx2), make_fc(mutate(text, 10))))

    query_content_id, query_fc = fcs.pop(0)
    store.put([(query_content_id, query_fc)])
    accumulating_predicate = nilsimsa_near_duplicates(
        label_store, store,
        threshold=100).set_query_id(query_content_id).create_predicate()

    start = time.time()
    results = filter(accumulating_predicate, fcs)
    elapsed = time.time() - start
    print '%d filtered to %d in %f seconds, %f per second' % (
        len(fcs), len(results), elapsed, len(fcs) / elapsed)
    assert len(results) == num_texts - 1 # minus the query


def test_geotime_filter():
    gc1 = GeoCoords({'foo': [(10, 10, 10, None)]})
    gc2 = GeoCoords({'foo': [(10, 10, 10, None), (-10, 10, 10, 10)]})
    gc3 = GeoCoords({'foo': [(-10, 10, 10, None), (10, 10, 10, 10)]})

    fc1 = FC()
    fc1['!co_LOC'] = gc1
    fc2 = FC()
    fc2['!co_LOC'] = gc2
    fc3 = FC()
    fc3['!co_LOC'] = gc3

    gt = geotime()
    gt.params = dict(min_lat=0, max_lat=20,
                     min_lon=-20, max_lon=0,
                     min_time=0)
    pred = gt.create_predicate()

    results = filter(pred, [('', fc1), ('', fc2), ('', fc3)])
    assert len(results) == 1
    assert results[0][1] == fc2
