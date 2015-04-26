'''dossier.web.filter_preds provides filter predicates.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from dossier.fc import FeatureCollection, StringCounter
from dossier.metrics.pairwise import nilsimsa_max_similarity


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
    if feature_name not in fc:
        return fc.get(FeatureCollection.DISPLAY_PREFIX + feature_name)
    else:
        return fc.get(feature_name)


def near_duplicates(
        label_store, store,
        feature_name = 'nilsimsa_all',
        kernel = nilsimsa_max_similarity,
        threshold = 119 / 128,
    ):
    '''Filter results that are highly similar to the query FC or any FC
    that was not filtered earlier in the stream.  "similarity" means
    that the output of `kernel(fc1, fc2, feature_name)` is above
    `threshold`.  These default to nilsimsa_max_similarity and
    119/128.  `feature_name` defaults to 'nilsimsa_all'.

    '''
    def init_filter(query_content_id):
        query_fc = store.get(query_content_id)
        sim_feature = get_string_counter(query_fc, feature_name)

        if sim_feature:
            accumulating_fc = FeatureCollection()
            accumulating_fc[feature_name] = StringCounter()
            accumulating_fc[feature_name].update(sim_feature)
        else:
            accumulating_fc = None

        def accumulating_predicate((content_id, fc)):
            if accumulating_fc is None:
                # query_fc lacked sim_feature, so filter nothing
                return False

            similarity = kernel(fc, accumulating_fc, feature_name)
            accumulating_fc[feature_name].update(get_string_counter(fc, feature_name))
            if similarity > threshold:
                return False
            else:
                return True

        return accumulating_predicate

    return init_filter
