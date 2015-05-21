'''dossier.web.filters provides search engine filters.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
from itertools import product

import nilsimsa

from dossier.fc import FeatureCollection, StringCounter
from dossier.web.interface import Filter


class already_labeled(Filter):
    '''Filter results that have a label associated with them.

    If a result has a *direct* label between it and the query, then
    it will be removed from the list of results.
    '''
    def __init__(self, label_store):
        super(already_labeled, self).__init__()
        self.label_store = label_store

    def create_predicate(self):
        labeled = self.label_store.directly_connected(self.query_content_id)
        labeled_cids = set(lab.other(self.query_content_id) for lab in labeled)
        return lambda (cid, _): cid not in labeled_cids


class nilsimsa_near_duplicates(Filter):
    '''Filter results that nilsimsa says are highly similar.

    To perform an filtering, this requires that the FCs carry
    StringCounter at `nilsimsa_feature_name` and results with
    nilsimsa comparison higher than the `threshold` are filtered.
    `threshold` defaults to 119, which is in the range [-128, 128] per
    the definition of nilsimsa. `nilsimsa_feature_name` defaults to
    'nilsimsa_all'.

    A note about speed performance: the order complexity of this filter
    is linear in the number of results that get through the filter.
    While that is unfortunate, it is inherent to the nature of using
    comparison-based locality sensitive hashing (LSH). Other LSH
    techniques, such as shingle hashing with simhash tend to have less
    fidelity, but can be efficiently indexed to allow O(1) lookups in a
    filter like this.

    Before refactoring this to use nilsimsa directly, this was using a
    "kernel" function that had nilsimsa buried inside it, and it had
    this kind of speed performance:

    dossier/web/tests/test_filter_preds.py::test_near_duplicates_speed_perf  4999 filtered to 49 in 2.838213 seconds, 1761.319555 per second

    After refactoring to use nilsimsa directly in this function, the
    constant factors get better, and the order complexity is still
    linear in the number of items that the filter has emitted, because
    it has to remember them and scan over them. Thresholding in the
    nilsimsa.compare_digests function helps considerably: four times
    faster on this synthetic test data when there are many different
    documents, which is the typical case:

    Without thresholding in the nilsimsa.compare_digests:
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 5049 filtered to 49 in 0.772274 seconds, 6537.834870 per second
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 1049 filtered to 49 in 0.162775 seconds, 6444.477004 per second
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 209 filtered to 9 in 0.009348 seconds, 22357.355097 per second

    With thresholding in the nilsimsa.compare_digests:
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 5049 filtered to 49 in 0.249705 seconds, 20219.853262 per second
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 1549 filtered to 49 in 0.112724 seconds, 13741.549025 per second
    dossier/web/tests/test_filter_preds.py::test_nilsimsa_near_duplicates_speed_perf 209 filtered to 9 in 0.009230 seconds, 22643.802754 per second
    '''
    def __init__(self, label_store, store,
                 nilsimsa_feature_name='nilsimsa_all', threshold=119):
        self.label_store = label_store
        self.store = store
        self.nilsimsa_feature_name = nilsimsa_feature_name
        self.threshold = threshold

    def create_predicate(self):
        query_fc = self.get_query_fc()
        sim_feature = get_string_counter(query_fc, self.nilsimsa_feature_name)

        accumulator = dict()
        if sim_feature:
            for nhash in sim_feature:
                accumulator[nhash] = self.query_content_id

        def accumulating_predicate((content_id, fc)):
            sim_feature = get_string_counter(fc, self.nilsimsa_feature_name)
            if not sim_feature:
                return True

            for nhash in sim_feature:
                if nhash in accumulator:
                    # either exact duplicate, or darn close (see
                    # test_nilsimsa_exact_match), so filter it and no
                    # need to update accumulator
                    return False

            for hash1, hash2 in product(sim_feature, accumulator):
                score = nilsimsa.compare_digests(hash1, hash2,
                                                 threshold=self.threshold)
                if score > self.threshold:
                    # near duplicate, so filter and do not accumulate
                    return False

            for nhash in sim_feature:
                accumulator[nhash] = content_id

            # allow it through
            return True

        return accumulating_predicate

    def get_query_fc(self):
        return self.store.get(self.query_content_id)


def get_string_counter(fc, feature_name):
    '''Find and return a :class:`~dossier.fc.StringCounter` at
    `feature_name` or at `DISPLAY_PREFIX` + `feature_name` in the
    `fc`, or return None.

    '''
    if feature_name not in fc:
        feature = fc.get(FeatureCollection.DISPLAY_PREFIX + feature_name)
    else:
        feature = fc.get(feature_name)
    if isinstance(feature, StringCounter):
        return feature
    else:
        return None
