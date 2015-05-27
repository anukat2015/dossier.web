from __future__ import absolute_import, division, print_function

import abc


class SearchEngine(object):
    '''Defines an interface for search engines.

    A search engine, at a high level, takes a query feature collection
    and returns a list of results, where each result is itself a
    feature collection.

    The return format should be a dictionary with at least one key,
    ``results``, which is a list of tuples of ``(content_id, FC)``,
    where ``FC`` is a :class:`dossier.fc.FeatureCollection`.
    '''
    __metaclass__ = abc.ABCMeta

    def param(name, cons=str, default=None):
        def fget(self):
            try:
                return cons(self.query_params.get(name, default))
            except (TypeError, ValueError):
                return default
        return property(fget=fget)

    def int_param(name, default=0, minimum=0, maximum=0):
        def fget(self):
            try:
                n = int(self.query_params.get(name, default))
                return min(maximum, max(minimum, n))
            except (TypeError, ValueError):
                return default
        return property(fget=fget)

    result_limit = int_param('limit', default=30, maximum=1000)

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
        self.query_content_id = None
        self.query_params = {}
        self._filters = {}

    def set_query_id(self, query_content_id):
        '''Set the query id for this search engine.

        This must be called before calling other methods like
        ``create_filter_predicate`` or ``recommendations``.
        '''
        self.query_content_id = query_content_id
        return self

    def set_query_params(self, query_params):
        '''Set the query parameters for this search engine.

        The exact set of query parameters is specified by the end user.

        :param query_params: query parameters
        :type query_params: ``name |--> str | [str]``
        '''
        self.query_params = query_params
        return self

    def add_filter(self, name, filter):
        '''Add a filter to this search engine.

        :param filter: A filter.
        :type filter: :class:`dossier.web.Filter`
        :rtype: self
        '''
        self._filters[name] = filter
        return self

    def create_filter_predicate(self):
        '''Creates a filter predicate.

        The list of available filters is given by calls to
        ``add_filter``, and the list of filters to use is given by
        parameters in ``query_params``.

        In this default implementation, multiple filters can be
        specified with the ``filter`` parameter. Each filter is
        initialized with the same set of query parameters given to the
        search engine.

        The returned function accepts a ``(content_id, FC)`` and
        returns ``True`` if and only if every selected predicate
        returns ``True`` on the same input.
        '''
        assert self.query_content_id is not None, \
                'must call SearchEngine.set_query_id first'

        filter_names = self.query_params.get('filter', [])
        if len(filter_names) == 0 and 'already_labeled' in self._filters:
            filter_names = ['already_labeled']
        init_filters = [(n, self._filters[n]) for n in filter_names]
        preds = [lambda _: True]
        for name, p in init_filters:
            preds.append(p.set_query_id(self.query_content_id)
                          .set_query_params(self.query_params)
                          .create_predicate())
        return lambda (cid, fc): fc is not None and all(p((cid, fc))
                                                        for p in preds)

    @property
    def result_limit(self):
        return min(1000, int(self.query_params.get('limit', 30)))

    @abc.abstractmethod
    def recommendations(self):
        '''Return recommendations.

        The return type is loosely specified. In particular, it must
        be a dictionary with at least one key, ``results``, which maps
        to a list of tuples of ``(content_id, FC)``. The returned
        dictionary may contain other keys.
        '''
        raise NotImplementedError()


class Filter(object):
    '''A filter predicate for results returned by search engines.

    A filter predicate is a :class:`yakonfig.Configurable` object
    (or one that can be auto-configured) that returns a callable
    for creating a predicate that will filter results produced by
    a search engine.
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.query_content_id = None
        self.query_params = {}

    def set_query_id(self, query_content_id):
        '''Set the query id.

        This is the identifier of the initial query. It can be useful
        when the filter predicate depends on state derived from the
        query.
        '''
        self.query_content_id = query_content_id
        return self

    def set_query_params(self, query_params):
        '''Set the query parameters.

        The exact set of query parameters is specified by the end user.

        :param query_params: query parameters
        :type query_params: ``name |--> str | [str]``
        '''
        self.query_params = query_params
        return self

    @abc.abstractmethod
    def create_predicate(self):
        '''Creates a predicate for this filter.

        The predicate should accept a tuple of ``(content_id, FC)``
        and return ``True`` if and only if the given result should be
        included in the list of recommendations provided to the user.
        '''
        raise NotImplementedError()
