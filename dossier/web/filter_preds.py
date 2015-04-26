'''dossier.web.filter_preds provides filter predicates.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
from itertools import product

import nilsimsa

from dossier.fc import FeatureCollection, StringCounter


def already_labeled(label_store):
    '''Filter results that have a label associated with them.

    If a result has a *direct* label between it and the query, then
    it will be removed from the list of results.
    '''
    def init_filter(query_content_id):
        labeled = label_store.directly_connected(query_content_id)
        labeled_cids = {label.other(query_content_id) for label in labeled}
        def p((content_id, fc)):
            return content_id not in labeled_cids
        return p
    return init_filter


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


def nilsimsa_near_duplicates(
        label_store, store,
        nilsimsa_feature_name = 'nilsimsa_all',
        threshold = 119,
    ):
    '''Filter results that nilsimsa says are highly similar to the query
    FC or any FC that was not filtered earlier in the stream.  To
    perform an filtering, this requires that the FCs carry
    StringCounter at `nilsimsa_feature_name` and results with nilsimsa
    comparison higher than the `threshold` are filtered.  `threshold`
    defaults to 119, which is in the range [-128, 128] per the
    definition of nilsimsa.  `nilsimsa_feature_name` defaults to
    'nilsimsa_all'.

    A note about speed performance: the order complexity of this
    filter is linear in the number of results that get through the
    filter.  While that is unfortunate, it is inherent to the nature
    of using comparison-based locality sensitive hashing (LSH).  Other
    LSH techniques, such as shingle hashing with simhash tend to have
    less fidelity, but can be efficiently indexed to allow O(1)
    lookups in a filter like this.

    Before refactoring this to use nilsimsa directly, this was using a
    "kernel" function that had nilsimsa buried inside it, and it had
    this kind of speed performance:

    dossier/web/tests/test_filter_preds.py::test_near_duplicates_speed_perf  4999 filtered to 49 in 2.838213 seconds, 1761.319555 per second

    After refactoring to use nilsimsa directly in this function, the
    constant factors get better, and the order complexity is still
    linear in the number of items that the filter has emitted, because
    it has to remember them and scan over them.  Thresholding in the
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
    def init_filter(query_content_id):
        query_fc = store.get(query_content_id)
        sim_feature = get_string_counter(query_fc, nilsimsa_feature_name)

        accumulator = dict()
        if sim_feature:
            for nhash in sim_feature:
                accumulator[nhash] = query_content_id

        def accumulating_predicate((content_id, fc)):

            sim_feature = get_string_counter(fc, nilsimsa_feature_name)
            for nhash in sim_feature:
                if nhash in accumulator:
                    ## either exact duplicate, or darn close (see
                    ## test_nilsimsa_exact_match), so filter it and no
                    ## need to update accumulator
                    return False

            for hash1, hash2 in product(sim_feature, accumulator):
                score = nilsimsa.compare_digests(hash1, hash2, threshold=threshold)
                if score > threshold:
                    ## near duplicate, so filter and do not accumulate
                    return False

            for nhash in sim_feature:
                accumulator[nhash] = content_id

            ## allow it through
            return True

        return accumulating_predicate

    return init_filter
