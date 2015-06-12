'''dossier.web.search_engines

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from itertools import ifilter, islice
import logging
import random as rand

from dossier.fc import SparseVector, StringCounter
from dossier.web.interface import SearchEngine


logger = logging.getLogger(__name__)


class random(SearchEngine):
    '''Return random results with the same name.

    This finds all content objects that have a matching name and
    returns ``limit`` results at random.

    If there is no ``NAME`` index defined, then this always returns
    no results.
    '''
    def __init__(self, store):
        super(random, self).__init__()
        self.store = store

    def recommendations(self):
        if u'NAME' not in self.store.index_names():
            return {'results': []}

        fc = self.store.get(self.query_content_id)
        if fc is None:
            raise KeyError(self.query_content_id)
        cids = []
        for name in fc.get(u'NAME'):
            cids.extend(self.store.index_scan(u'NAME', name))
        predicate = self.create_filter_predicate()
        results = list(ifilter(predicate, self.store.get_many(cids)))
        rand.shuffle(results)
        return {'results': results[0:self.params['limit']]}


class plain_index_scan(SearchEngine):
    '''Return a random sample of an index scan.

    This scans all indexes defined for all values in the query
    corresponding to those indexes.
    '''
    def __init__(self, store):
        super(plain_index_scan, self).__init__()
        self.store = store

    def recommendations(self):
        predicate = self.create_filter_predicate()
        cids = self.streaming_ids(self.query_content_id)
        results = ifilter(predicate,
                          ((cid, self.store.get(cid)) for cid in cids))
        sample = streaming_sample(
            results, self.params['limit'], self.params['limit'] * 10)
        return {'results': sample}

    def get_query_fc(self, content_id):
        query_fc = self.store.get(content_id)
        if query_fc is None:
            logger.info('Could not find FC for "%s"', content_id)
        return query_fc

    def streaming_ids(self, content_id):
        def scan(idx_name, val):
            for cid in self.store.index_scan(idx_name, val):
                if cid not in cids and cid not in blacklist:
                    cids.add(cid)
                    yield cid

        query_fc = self.get_query_fc(content_id)
        if query_fc is None:
            return

        blacklist = set([content_id])
        cids = set()
        logger.info('starting index scan (query content id: %s)', content_id)
        for idx_name in self.store.index_names():
            feat = query_fc.get(idx_name, None)
            if isinstance(feat, unicode):
                logger.info('[Unicode index: %s] scanning for "%s"',
                            idx_name, feat)
                for cid in scan(idx_name, feat):
                    yield cid
            elif isinstance(feat, (SparseVector, StringCounter)):
                for name in feat.iterkeys():
                    logger.info('[StringCounter index: %s] scanning for "%s"',
                                idx_name, name)
                    for cid in scan(idx_name, name):
                        yield cid


def streaming_sample(seq, k, limit=None):
    '''Streaming sample.

    Iterate over seq (once!) keeping k random elements with uniform
    distribution.

    As a special case, if ``k`` is ``None``, then ``list(seq)`` is
    returned.

    :param seq: iterable of things to sample from
    :param k: size of desired sample
    :param limit: stop reading ``seq`` after considering this many
    :return: list of elements from seq, length k (or less if seq is
             short)
    '''
    if k is None:
        return list(seq)

    seq = iter(seq)
    if limit is not None:
        k = min(limit, k)
        limit -= k
    result = list(islice(seq, k))
    for count, x in enumerate(islice(seq, limit), len(result)):
        if rand.random() < (1.0 / count):
            result[rand.randint(0, k-1)] = x
    return result
