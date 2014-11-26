'''dossier.web provides REST web services for Dossier Stack

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autofunction:: dossier.web.get_application
.. autofunction:: dossier.web.SearchEngine
.. autofunction:: dossier.web.Filter
.. autofunction:: dossier.web.Route

Here are the available search engines by default:

.. autofunction:: dossier.web.engine_random

Here are the available filter predicates by default:

.. autofunction:: dossier.web.filter_already_labeled

On occasion, you may need to access the configuration of ``dossier.web``
directly. This can be done by using a ``config`` parameter in one of
your routes, which will be filled automatically with an instance of the
:class:`Config` class documented below.

.. autoclass:: dossier.web.Config

Some useful utility functions.

.. autofunction:: dossier.web.streaming_sample

Web service for active learning
===============================
.. automodule:: dossier.web.routes

'''
from dossier.web.config import Config
from dossier.web.filter_preds import already_labeled as filter_already_labeled
from dossier.web.interface import SearchEngine, Filter, Route
from dossier.web.run import get_application, run_with_argv
from dossier.web.search_engines import random as engine_random
from dossier.web.search_engines import plain_index_scan as engine_index_scan
from dossier.web.search_engines import streaming_sample

__all__ = [
    'Config',
    'SearchEngine', 'Filter', 'Route',
    'get_application', 'run_with_argv',
    'filter_already_labeled', 'engine_random', 'engine_index_scan',
    'streaming_sample',
]
