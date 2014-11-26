'''dossier.web.filter_preds provides filter predicates.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function


def already_labeled(label_store):
    '''Filter results that have a label associated with them.

    If a result has a *direct* label between it and the query, then
    it will be removed from the list of results.
    '''
    def init_filter(query_content_id):
        labeled = label_store.get_all_for_content_id(query_content_id)
        labeled_cids = {label.other(query_content_id) for label in labeled}
        def p((content_id, fc)):
            return content_id not in labeled_cids
        return p
    return init_filter
