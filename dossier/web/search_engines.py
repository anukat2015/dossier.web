'''dossier.web.search_engines

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function

from itertools import ifilter, islice
import logging
import random as rand


logger = logging.getLogger(__name__)


def random(store):
    '''Return random results with the same name.

    This finds all content objects that have a matching name and
    returns ``limit`` results at random.
    '''
    def _(content_id, filter_pred, limit):
        fc = store.get(content_id)
        if fc is None:
            raise KeyError(content_id)
        cids = []
        for name in fc.get(u'NAME'):
            cids.extend(store.index_scan(u'NAME', name))
        results = list(ifilter(
            lambda (cid, fc): fc is not None and filter_pred((cid, fc)),
            store.get_many(cids)))
        rand.shuffle(results)
        return {'results': results[0:limit]}
    return _


def plain_index_scan(store):
    '''Return a random sample of an index scan.

    This scans all indexes defined for all values in the query
    corresponding to those indexes.
    '''
    def streaming_ids(content_id):
        query_fc = store.get(content_id)
        if query_fc is None:
            logger.info('Could not find FC for "%s"', content_id)
            return

        blacklist = {content_id}
        cids = set()
        for idx_name in store.index_names():
            for name in query_fc.get(idx_name, {}):
                logger.info('index scanning for "%s" (content id: %s)',
                            name, content_id)
                for cid in store.index_scan(idx_name, name):
                    if cid not in cids and cid not in blacklist:
                        cids.add(cid)
                        yield cid

    def _(content_id, filter_pred, limit):
        cids = streaming_ids(content_id)
        results = ifilter(lambda (cid, fc):
                              fc is not None and filter_pred((cid, fc)),
                          ((cid, store.get(cid)) for cid in cids))
        return {'results': streaming_sample(results, limit, limit * 10)}
    return _


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
