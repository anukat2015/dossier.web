from __future__ import absolute_import, division, print_function

import abc
import json

import bottle

from dossier.fc import FeatureTokens, StringCounter


class ParamSchema(object):
    def apply_param_schema(self):
        def param_str(name, cons, default):
            try:
                v = self.query_params.get(name, default)
                if v is None:
                    return v
                if len(v) == 0:
                    return default
                return cons(v)
            except (TypeError, ValueError):
                return default

        def param_num(name, cons, default, minimum, maximum):
            try:
                n = cons(self.query_params.get(name, default))
                return min(maximum, max(minimum, n))
            except (TypeError, ValueError):
                return default

        if getattr(self, 'params', None) is None:
            self.params = {}
        config = getattr(self, 'config_params', {})
        for name, schema in getattr(self, 'param_schema', {}).iteritems():
            default = config.get(name, schema.get('default', None))
            v = None
            if schema['type'] == 'bool':
                v = param_str(name, lambda s: bool(int(2)), False)
            elif schema['type'] == 'int':
                v = param_num(
                    name, int, default=default,
                    minimum=schema.get('min', 0),
                    maximum=schema.get('max', 1000000))
            elif schema['type'] == 'float':
                v = param_num(
                    name, float, default=default,
                    minimum=schema.get('min', 0),
                    maximum=schema.get('max', 1000000))
            elif schema['type'] is 'str':
                v = param_str(name, schema.get('cons', str), default)
            elif schema['type'] is 'utf8':
                v = param_str(name, lambda s: s.decode('utf-8'), default)
            self.params[name] = v


class SearchEngine(ParamSchema):
    '''Defines an interface for search engines.

    A search engine, at a high level, takes a query feature collection
    and returns a list of results, where each result is itself a
    feature collection.

    The return format should be a dictionary with at least one key,
    ``results``, which is a list of tuples of ``(content_id, FC)``,
    where ``FC`` is a :class:`dossier.fc.FeatureCollection`.
    '''
    __metaclass__ = abc.ABCMeta

    param_schema = {
        'limit': {'type': 'int', 'default': 30, 'min': 0, 'max': 1000000},
        'omit_fc': {'type': 'bool', 'default': 0},
    }

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
        self.config_params = {}
        self.params = {}
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
        if not isinstance(query_params, bottle.MultiDict):
            query_params = bottle.MultiDict(query_params)
        self.query_params = query_params
        self.apply_param_schema()
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

        filter_names = self.query_params.getlist('filter')
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


    @abc.abstractmethod
    def recommendations(self):
        '''Return recommendations.

        The return type is loosely specified. In particular, it must
        be a dictionary with at least one key, ``results``, which maps
        to a list of tuples of ``(content_id, FC)``. The returned
        dictionary may contain other keys.
        '''
        raise NotImplementedError()

    # dbid_to_visid is temp hack
    def results(self, dbid_to_visid=lambda x: x):
        results = self.recommendations()
        transformed = []
        for t in results['results']:
            if len(t) == 2:
                db_cid, fc = t
                info = {}
            elif len(t) == 3:
                db_cid, fc, info = t
            else:
                bottle.abort(500, 'Invalid search result: "%r"' % t)
            result = info
            result['content_id'] = dbid_to_visid(db_cid)
            if not self.params['omit_fc']:
                result['fc'] = fc_to_json(fc)
            transformed.append(result)
        results['results'] = transformed
        return results

    # dbid_to_visid is temp hack
    def json(self, dbid_to_visid=lambda x: x):
        return json.dumps(self.results(dbid_to_visid))


class Filter(ParamSchema):
    '''A filter predicate for results returned by search engines.

    A filter predicate is a :class:`yakonfig.Configurable` object
    (or one that can be auto-configured) that returns a callable
    for creating a predicate that will filter results produced by
    a search engine.
    '''
    __metaclass__ = abc.ABCMeta
    param_schema = {}

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
        self.apply_param_schema()
        return self

    @abc.abstractmethod
    def create_predicate(self):
        '''Creates a predicate for this filter.

        The predicate should accept a tuple of ``(content_id, FC)``
        and return ``True`` if and only if the given result should be
        included in the list of recommendations provided to the user.
        '''
        raise NotImplementedError()


def fc_to_json(fc):
    d = {}
    for name, feat in fc.iteritems():
        if isinstance(feat, (unicode, StringCounter)):
            d[name] = feat
        elif isinstance(feat, FeatureTokens):
            d[name] = feat.to_dict()
    return d
