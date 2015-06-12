'''
Search engine and filter interfaces
===================================

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autoclass:: SearchEngine
    :show-inheritance:
.. autoclass:: Filter
    :show-inheritance:
.. autoclass:: Queryable
'''

from __future__ import absolute_import, division, print_function

import abc
import json

import bottle

from dossier.web import util


class Queryable(object):
    '''Queryable supports parameterization from URLs and config.

    Queryable is meant to be subclassed by things that have two
    fundamental things in common:

      1. Requires a single query identifier.
      2. Can be optionally configured from either user provided
         URL parameters or admin provided configuration.

    Queryable provides a common interface for these two things, while
    also providing a way to declare a schema for the parameters. This
    schema is used to convert values from the URL/config into typed
    Python values.

    **Parameter schema**

    The ``param_schema`` class variable can define rudimentary type
    conversion from strings to typed Python values such as ``unicode``
    or ``int``.

    ``param_schema`` is a dictionary that maps keys (parameter name) to
    a parameter type. A parameter type is itself a dictionary with the
    following keys:

    **type**
        Required. Must be one of ``'bool'``, ``'int'``,
        ``'float'``, ``'bytes'`` or ``'unicode'``.
    **min**
        Optional for ``'int'`` and ``'float'`` types.
        Specifies a minimum value.
    **max**
        Optional for ``'int'`` and ``'float'`` types.
        Specifies a maximum value.
    **encoding**
        Specifies an encoding for ``'unicode'`` types.

    If you want to inherit the schema of a parent class, then you
    can use:

    .. code-block:: python

        param_schema = dict(ParentClass.param_schema, **{
            # your extra types here
        })

    :ivar query_content_id: The query content id.
    :ivar query_params: The raw query parameters,
                        as a :class:`bottle.MultiDict`.
    :ivar config_params: The raw configuration parameters.
                         This must be maintained explicitly, but will
                         be incorporated in the values for ``params``.
                         If ``k`` is and ``config_params``, then
                         ``k``'s default value is ``config_params[k]``
                         (which is overridden by ``query_params[k]`` if
                         it exists).
    :ivar params: The combined and typed values of ``query_params``
                  and ``config_params``.

    .. automethod:: __init__
    .. automethod:: set_query_id
    .. automethod:: set_query_params
    .. automethod:: add_query_params
    '''
    param_schema = {}

    def __init__(self):
        '''Creates a new instance of :class:`Queryable`.

        This initializes a default empty state, where all parameter
        dictionaries are empty and ``query_content_id`` is ``None``.

        To take advantage of dependency injected configuration, you'll
        want to write your own constructor that sets config parameters
        explicitly:

        .. code-block:: python

            def __init__(self, param1=None, param2=5):
                self.config_params = {
                    'param1': param1,
                    'param2': param2,
                }
                super(MyClass, self).__init__()

        It's important to call the constructor after ``config_params``
        has been set so that the schema is applied correctly.
        '''
        self.query_content_id = None
        self.query_params = {}
        if not hasattr(self, 'config_params'):
            self.config_params = {}
        self.params = {}
        self.apply_param_schema()

    def set_query_id(self, query_content_id):
        '''Set the query id.

        :param str query_content_id: The query content identifier.
        :rtype: :class:`Queryable`
        '''
        self.query_content_id = query_content_id
        return self

    def set_query_params(self, query_params):
        '''Set the query parameters.

        The query parameters should be a dictionary mapping keys to
        strings or lists of strings.

        :param query_params: query parameters
        :type query_params: ``name |--> (str | [str])``
        :rtype: :class:`Queryable`
        '''
        self.query_params = as_multi_dict(query_params)
        self.apply_param_schema()
        return self

    def add_query_params(self, query_params):
        '''Overwrite the given query parameters.

        This is the same as :meth:`Queryable.set_query_params`,
        except it overwrites existing parameters individually
        whereas ``set_query_params`` deletes all existing key in
        ``query_params``.
        '''
        query_params = as_multi_dict(query_params)
        for k in query_params:
            self.query_params.pop(k, None)
            for v in query_params.getlist(k):
                self.query_params[k] = v
        self.apply_param_schema()
        return self

    def apply_param_schema(self):
        '''Applies the schema defined to the given parameters.

        This combines the values in ``config_params`` and
        ``query_params``, and converts them to typed Python values per
        ``param_schema``.

        This is called automatically whenever the query parameters are
        updated.
        '''
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

        for name, schema in getattr(self, 'param_schema', {}).iteritems():
            default = self.config_params.get(name, schema.get('default', None))
            v = None
            if schema['type'] == 'bool':
                v = param_str(name, lambda s: bool(int(s)), False)
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
            elif schema['type'] is 'bytes':
                v = param_str(name, schema.get('cons', str), default)
            elif schema['type'] is 'unicode':
                encoding = schema.get('encoding', 'utf-8')
                v = param_str(name, lambda s: s.decode(encoding), default)
            self.params[name] = v


class SearchEngine(Queryable):
    '''Defines an interface for search engines.

    A search engine, at a high level, takes a query feature collection
    and returns a list of results, where each result is itself a
    feature collection.

    Note that this is an abstract class. Implementors must provide the
    :meth:`SearchEngine.recommendations` method.

    .. automethod:: __init__
    .. automethod:: recommendations
    .. automethod:: results
    .. automethod:: respond
    .. automethod:: add_filter
    .. automethod:: create_filter_predicate
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

        * **kvlclient**: :class:`kvlayer._abstract_storage.AbstractStorage`
        * **store**: :class:`dossier.store.Store`
        * **label_store**: :class:`dossier.label.LabelStore`

        If you want to expand the set of items that can be injected,
        then you must subclass :class:`dossier.web.Config`, define your
        new services as instance attributes, and set your new config
        instance with :meth:`dossier.web.Config.set_config`.

        :rtype: A callable with a signature isomorphic to
                :meth:`dossier.web.SearchEngine.__call__`.
        '''
        super(SearchEngine, self).__init__()
        self._filters = {}

    def add_filter(self, name, filter):
        '''Add a filter to this search engine.

        :param filter: A filter.
        :type filter: :class:`Filter`
        :rtype: :class:`SearchEngine`
        '''
        self._filters[name] = filter
        return self

    def create_filter_predicate(self):
        '''Creates a filter predicate.

        The list of available filters is given by calls to
        ``add_filter``, and the list of filters to use is given by
        parameters in ``params``.

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

    def results(self):
        '''Returns results as a JSON encodable Python value.

        This calls :meth:`SearchEngine.recommendations` and converts
        the results returned into JSON encodable values. Namely,
        feature collections are slimmed down to only features that
        are useful to an end-user.
        '''
        results = self.recommendations()
        transformed = []
        for t in results['results']:
            if len(t) == 2:
                cid, fc = t
                info = {}
            elif len(t) == 3:
                cid, fc, info = t
            else:
                bottle.abort(500, 'Invalid search result: "%r"' % t)
            result = info
            result['content_id'] = cid
            if not self.params['omit_fc']:
                result['fc'] = util.fc_to_json(fc)
            transformed.append(result)
        results['results'] = transformed
        return results

    def respond(self, response):
        '''Perform the actual web response.

        This is usually just a JSON encoded dump of the search results,
        but implementors may choose to implement this differently
        (e.g., with a cache).

        :param response: A web response object.
        :type response: :class:`bottle.Response`
        :rtype: `str`
        '''
        response.content_type = 'application/json'
        return json.dumps(self.results())


class Filter(Queryable):
    '''A filter for results returned by search engines.

    A filter is a :class:`yakonfig.Configurable` object
    (or one that can be auto-configured) that returns a callable
    for creating a predicate that will filter results produced by
    a search engine.

    A filter has one abstract method: :meth:`Filter.create_predicate`.

    .. automethod:: create_predicate
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_predicate(self):
        '''Creates a predicate for this filter.

        The predicate should accept a tuple of ``(content_id, FC)``
        and return ``True`` if and only if the given result should be
        included in the list of recommendations provided to the user.
        '''
        raise NotImplementedError()


def as_multi_dict(d):
    'Coerce a dictionary to a bottle.MultiDict'
    if isinstance(d, bottle.MultiDict):
        return d
    md = bottle.MultiDict()
    for k, v in d.iteritems():
        if isinstance(v, list):
            for x in v:
                md[k] = x
        else:
            md[k] = v
    return md
