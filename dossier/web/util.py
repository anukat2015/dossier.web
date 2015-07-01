'''Utility functions that don't belong elsewhere.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

from dossier.fc import \
    FeatureCollection, FeatureTokens, StringCounter, GeoCoords


def fc_to_json(fc):
    # If `fc` has already been converted to a dict elsewhere, then
    # don't try to do it again.
    if not isinstance(fc, FeatureCollection):
        return fc
    d = {}
    for name, feat in fc.iteritems():
        if isinstance(feat, (unicode, StringCounter, dict)):
            d[name] = feat
        elif isinstance(feat, FeatureTokens):
            d[name] = feat.to_dict()
        elif is_filterable_geo_feature(name, feat):
            d[name] = feat.to_dict()
    return d


def is_filterable_geo_feature(name, feat):
    want = FeatureCollection.GEOCOORDS_PREFIX + 'both_co_LOC_1'
    return isinstance(feat, GeoCoords) and name == want
