from __future__ import absolute_import, division, print_function

import abc


class SearchEngine(object):
    '''Defines an interface for search engines.

    A search engine, at a high level, takes a query feature collection
    and returns a list of results, where each result is itself a
    feature collection.

    More generally, a search engine is a :class:`yakonfig.Configurable`
    object (or one that can be auto-configured) that returns a callable
    that performs the actual searching.

    Here's an example of a simple search engine that returns the
    results of an index scan:

    .. code-block:: python

        def search_by_name(store):
            def _(content_id, filter_pred, limit):
                fc = store.get(content_id)
                cids = []
                for name in fc.get(u'NAME'):
                    cids.extend(store.index_scan(u'NAME', name))
                results = list(filter(filter_pred, store.get_many(cids)))
                return {
                    'results': results[0:int(limit)],
                }
            return _

    .. automethod:: dossier.web.SearchEngine.__init__
    .. automethod:: dossier.web.SearchEngine.__call__
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self):
        '''Create a new search engine.

        The creation of a search engine is distinct from the operation
        of a search engine. Namely, the creation of a search engine
        is subject to dependency injection. The following parameters
        are special in that they will be automatically populated with
        special values if present in your ``__init__``:

        * **kvlclient**:
          :class:`kvlayer._abstract_storage.AbstractStorage`
        * **store**: :class:`dossier.store.Store`
        * **label_store**: :class:`dossier.label.LabelStore`

        :rtype: A callable with a signature isomorphic to
                :meth:`dossier.web.SearchEngine.__call__`.
        '''
        pass

    @abc.abstractmethod
    def __call__(content_id, filter_pred, limit, **kwargs):
        '''Run a search engine.

        This method runs a search engine with the query ``content_id``
        and returns a result set of feature collections.

        ``content_id`` will correspond to a feature collection
        accessible via a :class:`dossier.store.Store`. Note
        that ``content_id`` may not point to any existing
        :class:`dossier.fc.FeatureCollection`. If a feature collection
        does not exist, a search engine may invent one or simply return
        no results.

        ``filter_pred`` is a predicate that returns ``True`` given a
        (``content_id``, :class:`dossier.fc.FeatureCollection`) if and
        only if that object should appear in the results.  Typically,
        this filtering predicate is used to make sure already labeled
        feature collections don't re-appear.

        ``limit`` is an integer that determines how many results the
        user wants to handle. Search engines may assume that this is
        capped at a reasonable maximum.

        Finally, any additional query parameters in the URL are passed
        as keyword arguments, which will all be strings.
        '''
        pass


class Filter(object):
    '''A filter predicate for results returned by search engines.

    A filter predicate is a :class:`yakonfig.Configurable` object
    (or one that can be auto-configured) that returns a callable
    for creating a predicate that will filter results produced by
    a search engine.

    The predicate should be a function that accepts a tuple
    (``content_id``, :class:`dossier.fc.FeatureCollection`) and returns
    ``True`` if and only if that result should be included in the
    recommendations presented to the user.

    Here is how the :func:`dossier.web.filter_already_labeled` filter
    is written:

    .. code-block:: python

        def already_labeled(label_store):
            def init_filter(query_content_id):
                labeled = label_store.get_all_for_content_id(
                    query_content_id)
                labeled_cids = {label.other(query_content_id)
                                for label in labeled}
                def p((content_id, fc)):
                    return content_id not in labeled_cids
                return p
            return init_filter

    Note that fetching all of the labels before running the predicate
    is critical for this to perform well.
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self):
        '''Create a new filter.

        The creation of a filter is distinct from the operation of
        a filter. Namely, the creation of a filter is subject to
        dependency injection. The following parameters are special in
        that they will be automatically populated with special values
        if present in your ``__init__``:

        * **kvlclient**:
          :class:`kvlayer._abstract_storage.AbstractStorage`
        * **store**: :class:`dossier.store.Store`
        * **label_store**: :class:`dossier.label.LabelStore`

        :rtype: A callable with a signature isomorphic to
                :meth:`dossier.web.Filter.__call__`.
        '''
        pass

    @abc.abstractmethod
    def __call__(self, query_content_id):
        '''Create a filter predicate function.

        The reason for the extra level of indirection is so that you
        may run "initialization" code for a particular query. See
        :meth:`dossier.web.Filter` for an example.

        :param str query_content_id: The content id of the query.
                                     This *may* correspond to an existing
                                     feature collection.
        :rtype: A function with type
                ``(content_id, FeatureCollection) -> bool``.
        '''
        pass


class Route(object):
    '''Defines an interface for web routes.

    A web route is a function that receives HTTP requests and returns
    HTTP responses.

    You may add your own routes to a ``dossier.web`` application by
    defining Python objects that conform to the interface defined here
    (they do not need to subclass this class).

    .. automethod:: dossier.web.Route.__init__
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, bottle_app):
        '''Add routes to an existing Bottle application.

        This interface permits one to add new routes to the
        Bottle application used by ``dossier.web``. For example:

        .. code-block:: python

            def my_route(bottle_app):
                @bottle_app.get('/visit-me')
                def bottle_route():
                    return 'Hello, world!'

            # Use it in the application.
            _, app = get_application(routes=[my_route])

        ``dossier.web`` also configures dependency injection for your
        routes. The following is a list of special parameter names that
        can appear in your route function. They will be automatically
        populated with values of the following types:

        * **config**: :class:`dossier.web.Config`
        * **kvlclient**:
          :class:`kvlayer._abstract_storage.AbstractStorage`
        * **store**: :class:`dossier.store.Store`
        * **label_store**: :class:`dossier.label.LabelStore`
        * **search_engines**: ``list`` of search engines (which are
          duck typed to :class:`dossier.web.SearchEngine`).
        * **filter_preds**: ``list`` of filter predicates (which are
          duck typed to :class:`dossier.web.Filter`).
        * **request**: :class:`bottle.Request`
        * **response**: :class:`bottle.Response`

        :param bottle_app: A Bottle application.
        :type bottle_app: :class:`bottle.Bottle`
        '''
        pass
