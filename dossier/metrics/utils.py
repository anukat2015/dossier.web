'''helper utilities for kernel functions

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import

from dossier.fc import FeatureCollection, StringCounter


def require_string_counters(kernel):
    '''decorator for kernels that require StringCounter features.

    '''
    def get_string_counters(fc1, fc2, feature_name, feature_name_other=None):
        if feature_name_other is None:
            feature_name_other = feature_name

        f1 = fc1.get(feature_name)
        f2 = fc2.get(feature_name_other)

        if not isinstance(f1, StringCounter):
            ## look for the same feature prefixed
            ## by the display string
            f1 = fc1.get(FeatureCollection.DISPLAY_PREFIX + feature_name)

        if not isinstance(f2, StringCounter):
            f2 = fc2.get(FeatureCollection.DISPLAY_PREFIX + feature_name_other)

        if not (f1 and f2):
            return 0.

        return kernel(f1, f2)

    return get_string_counters

