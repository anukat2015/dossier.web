'''kernels for comparing features using locality sensitive hashing

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division
from itertools import product

from dossier.fc import FeatureCollection, StringCounter
import nilsimsa

from dossier.metrics.utils import require_string_counters


@require_string_counters
def nilsimsa_max_similarity(f1, f2, threshold=None):
    '''returns the maximum of `nilsimsa.compare_digests(hash1, hash2)`
    over all pairs of hashes as the keys stored in
    `fc1[feature_name]` and `fc2[feature_name_other or feature_name]`

    This uses the `require_string_counters` decorator to get the
    StringCounter features out of `fc1 and `fc2`.

    If provided, `threshold` allows faster performance by returning
    any score over the threshold, which might *not* be the maximum.

    '''
    if threshold is not None:
        _threshold = int(threshold * 128)
    scores = []
    for hash1, hash2 in product(f1.keys(), f2.keys()):
        score = nilsimsa.compare_digests(hash1, hash2)
        if score == 128: return 1.0
        #if threshold is not None and score > _threshold: return score / 128
        scores.append(score)

    if not scores: 
        return 0.0
    else:
        return max(scores) / 128
