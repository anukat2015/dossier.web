from __future__ import absolute_import, division, print_function

from dossier.fc import FeatureCollection, FeatureTokens, StringCounter


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
    return d
