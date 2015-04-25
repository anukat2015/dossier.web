'''kernels for comparing features using locality sensitive hashing

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division

from dossier.fc import FeatureCollection, StringCounter
import nilsimsa

from dossier.metrics.pairwise import nilsimsa_max_similarity


def test_nilsimsa_max_similarity():

    fc1 = FeatureCollection()
    fc1['#nilsimsa_all'] = StringCounter([nilsimsa.Nilsimsa(text).hexdigest()
                                          for text in ['dog', 'cat', 'bird']])
    
    fc2 = FeatureCollection()
    fc2['#nilsimsa_all'] = StringCounter([nilsimsa.Nilsimsa(text).hexdigest()
                                          for text in ['doggy', 'kitty', 'birdie']])

    score = nilsimsa_max_similarity(fc1, fc2, 'nilsimsa_all')
    assert score > 0.9
