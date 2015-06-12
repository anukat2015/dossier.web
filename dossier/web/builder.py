'''A builder for constructing DossierStack web applications.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function

import inspect
import json
import logging

import bottle

from dossier.web import search_engines as builtin_engines
from dossier.web.config import Config
from dossier.web.filters import already_labeled
from dossier.web.routes import app as default_app


logger = logging.getLogger(__name__)


class WebBuilder(object):
    '''A builder for constructing DossierStack web applications.

    DossierStack web services have a lot of knobs, so instead of a single
    function with a giant list of parameters, we get a "builder" that
    lets one mutably construct data for building a web application.

    These "knobs" include, but are not limited to: adding routes or other
    Bottle applications, injecting services into routes, setting a URL
    prefix and adding filters and search engines.

    .. automethod:: __init__
    .. automethod:: get_app
    .. automethod:: mount
    .. automethod:: set_config
    .. automethod:: add_search_engine
    .. automethod:: add_filter
    .. automethod:: add_routes
    .. automethod:: inject
    .. automethod:: enable_cors
    '''
    def __init__(self, add_default_routes=True):
        '''Introduce a new builder.

        You can use method chaining to configure your web application
        options. e.g.,

        .. code-block:: python

            app = WebBuilder().enable_cors().get_app()
            app.run()

        This code will create a new Bottle web application that enables
        CORS (Cross Origin Resource Sharing).

        If ``add_default_routes`` is ``False``, then the default set of
        routes in ``dossier.web.routes`` is not added. This is only useful
        if you want to compose multiple Bottle applications constructed
        through multiple instances of ``WebBuilder``.
        '''
        self.app = bottle.Bottle()
        self.search_engines = {
            'random': builtin_engines.random,
            'plain_index_scan': builtin_engines.plain_index_scan,
        }
        self.filters = {
            'already_labeled': already_labeled,
        }
        self.mount_prefix = None
        self.config = None
        if add_default_routes:
            self.add_routes(default_app)

        # DEPRECATED. Remove. ---AG
        self.visid_to_dbid, self.dbid_to_visid = lambda x: x, lambda x: x

    def get_app(self):
        '''Eliminate the builder by producing a new Bottle application.

        This should be the final call in your method chain. It uses all
        of the built up options to create a new Bottle application.

        :rtype: :class:`bottle.Bottle`
        '''
        if self.config is None:
            # If the user never sets a config instance, then just create
            # a default.
            self.config = Config()
        if self.mount_prefix is None:
            self.mount_prefix = self.config.config.get('url_prefix')

        self.inject('config', lambda: self.config)
        self.inject('kvlclient', lambda: self.config.kvlclient)
        self.inject('store', lambda: self.config.store)
        self.inject('label_store', lambda: self.config.label_store)
        self.inject('search_engines', lambda: self.search_engines)
        self.inject('filters', lambda: self.filters)
        self.inject('request', lambda: bottle.request)
        self.inject('response', lambda: bottle.response)

        # DEPRECATED. Remove. ---AG
        self.inject('visid_to_dbid', lambda: self.visid_to_dbid)
        self.inject('dbid_to_visid', lambda: self.dbid_to_visid)

        # Also DEPRECATED.
        self.inject('label_hooks', lambda: [])

        # Load routes defined in entry points.
        for extroute in self.config.config.get('external_routes', []):
            mod, fun_name = extroute.split(':')
            logger.info('Loading external route: %s', extroute)
            fun = getattr(__import__(mod, fromlist=[fun_name]), fun_name)
            self.add_routes(fun())

        # This adds the `json=True` feature on routes, which always coerces
        # the output to JSON. Bottle, by default, only permits dictionaries
        # to be JSON, which is the correct behavior. (Because returning JSON
        # arrays is a hazard.)
        #
        # So we should fix the routes and then remove this. ---AG
        self.app.install(JsonPlugin())

        # Throw away the app and return it. Because this is elimination!
        app = self.app
        self.app = None
        if self.mount_prefix is not None:
            root = bottle.Bottle()
            root.mount(self.mount_prefix, app)
            return root
        else:
            return app

    def mount(self, prefix):
        '''Mount the application on to the given URL prefix.

        :param str prefix: A URL prefixi
        :rtype: :class:`WebBuilder`
        '''
        self.mount_prefix = prefix
        return self

    def set_config(self, config_instance):
        '''Set the config instance.

        By default, this is an instance of :class:`dossier.web.Config`,
        which provides services like ``kvlclient`` and
        ``label_store``. Custom services should probably subclass
        :class:`dossier.web.Config`, but it's not strictly necessary so
        long as it provides the same set of services (which are used
        for dependency injection into Bottle routes).

        :param config_instance: A config instance.
        :type config_instance: :class:`dossier.web.Config`
        :rtype: :class:`WebBuilder`
        '''
        self.config = config_instance
        return self

    def add_search_engine(self, name, engine):
        '''Adds a search engine with the given name.

        ``engine`` must be the **class** object rather than
        an instance. The class *must* be a subclass of
        :class:`dossier.web.SearchEngine`, which should provide a means
        of obtaining recommendations given a query.

        The ``engine`` must be a class so that its dependencies can be
        injected when the corresponding route is executed by the user.

        If ``engine`` is ``None``, then it removes a possibly existing
        search engine named ``name``.

        :param str name: The name of the search engine. This appears
                         in the list of search engines provided to the
                         user, and is how the search engine is invoked
                         via REST.
        :param engine: A search engine *class*.
        :type engine: `type`
        :rtype: :class:`WebBuilder`
        '''
        if engine is None:
            self.search_engines.pop(name, None)
        self.search_engines[name] = engine
        return self

    def add_filter(self, name, filter):
        '''Adds a filter with the given name.

        ``filter`` must be the **class** object rather than
        an instance. The class *must* be a subclass of
        :class:`dossier.web.Filter`, which should provide a means
        of creating a predicate function.

        The ``filter`` must be a class so that its dependencies can be
        injected when the corresponding route is executed by the user.

        If ``filter`` is ``None``, then it removes a possibly existing
        filter named ``name``.

        :param str name: The name of the filter. This is how the search engine
                         is invoked via REST.
        :param engine: A filter *class*.
        :type engine: `type`
        :rtype: :class:`WebBuilder`
        '''
        if name is None:
            self.filters.pop(name, None)
        self.filters[name] = filter
        return self

    def add_routes(self, routes):
        '''Merges a Bottle application into this one.

        :param routes: A Bottle application or a sequence of routes.
        :type routes: :class:`bottle.Bottle` or `[bottle route]`.
        :rtype: :class:`WebBuilder`
        '''
        # Basically the same as `self.app.merge(routes)`, except this
        # changes the owner of the route so that plugins on `self.app`
        # apply to the routes given here.
        if isinstance(routes, bottle.Bottle):
            routes = routes.routes
        for route in routes:
            route.app = self.app
            self.app.add_route(route)
        return self

    def inject(self, name, closure):
        '''Injects ``closure()`` into ``name`` parameters in routes.

        This sets up dependency injection for parameters named ``name``.
        When a route is invoked that has a parameter ``name``, then
        ``closure()`` is passed as that parameter's value.

        (The closure indirection is so the caller can control the time
        of construction for objects. For example, you may want to check
        the health of a database connection.)

        :param str name: Parameter name.
        :param function closure: A function with no parameters.
        :rtype: :class:`WebBuilder`
        '''
        self.app.install(create_injector(name, closure))
        return self

    def enable_cors(self):
        '''Enables Cross Origin Resource Sharing.

        This makes sure the necessary headers are set so that this
        web application's routes can be accessed from other origins.

        :rtype: :class:`WebBuilder`
        '''
        def access_control_headers():
            bottle.response.headers['Access-Control-Allow-Origin'] = '*'
            bottle.response.headers['Access-Control-Allow-Methods'] = \
                'GET, POST, PUT, DELETE, OPTIONS'
            bottle.response.headers['Access-Control-Allow-Headers'] = \
                'Origin, X-Requested-With, Content-Type, Accept, Authorization'

        def options_response(res):
            if bottle.request.method == 'OPTIONS':
                new_res = bottle.HTTPResponse()
                new_res.headers['Access-Control-Allow-Origin'] = '*'
                new_res.headers['Access-Control-Allow-Methods'] = \
                    bottle.request.headers.get(
                        'Access-Control-Request-Method', '')
                new_res.headers['Access-Control-Allow-Headers'] = \
                    bottle.request.headers.get(
                        'Access-Control-Request-Headers', '')
                return new_res
            res.headers['Allow'] += ', OPTIONS'
            return bottle.request.app.default_error_handler(res)

        self.app.add_hook('after_request', access_control_headers)
        self.app.error_handler[int(405)] = options_response
        return self

    def set_visid_to_dbid(self, f):
        'DEPRECATED. DO NOT USE.'
        self.visid_to_dbid = f
        return self

    def set_dbid_to_visid(self, f):
        'DEPRECATED. DO NOT USE.'
        self.dbid_to_visid = f
        return self


def create_injector(param_name, fun_param_value):
    '''Dependency injection with Bottle.

    This creates a simple dependency injector that will map
    ``param_name`` in routes to the value ``fun_param_value()``
    each time the route is invoked.

    ``fun_param_value`` is a closure so that it is lazily evaluated.
    This is useful for handling thread local services like database
    connections.

    :param str param_name: name of function parameter to inject into
    :param fun_param_value: the value to insert
    :type fun_param_value: a closure that can be applied with zero
                           arguments
    '''
    class _(object):
        api = 2

        def apply(self, callback, route):
            if param_name not in inspect.getargspec(route.callback)[0]:
                return callback

            def _(*args, **kwargs):
                pval = fun_param_value()
                if pval is None:
                    logger.error('service "%s" unavailable', param_name)
                    bottle.abort(503, 'service "%s" unavailable' % param_name)
                    return
                kwargs[param_name] = pval
                return callback(*args, **kwargs)
            return _
    return _()


class JsonPlugin(object):
    '''A custom JSON plugin for Bottle.

    Bottle has this functionality by default, but it is only triggered
    when the return value of a route is a ``dict``. This permits the
    programmer to write `json=True` into the route decorator, which
    causes the response to *always* be JSON.

    Basically, it just wraps the return value in ``json.dumps`` and
    sets the HTTP content type header appropriately.
    '''
    api = 2
    name = 'json_response'

    def apply(self, callback, route):
        if not route.config.get('json', False):
            return callback

        def _(*args, **kwargs):
            bottle.response.content_type = 'application/json'
            return json.dumps(callback(*args, **kwargs), indent=2)
        return _


def add_cli_arguments(p):
    p.add_argument('--bottle-debug', action='store_true',
                   help='Enable Bottle\'s debug mode.')
    p.add_argument('--reload', action='store_true',
                   help='Enable Bottle\'s reloading functionality.')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--host', default='localhost')
    p.add_argument('--server', default='wsgiref',
                   help='The web server to use. You only need to change this '
                        'if you\'re running a production server.')
