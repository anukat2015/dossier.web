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
    def __init__(self, add_default_routes=True):
        self.app = BottleAppFixScriptName()
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
            root = BottleAppFixScriptName()
            root.mount(self.mount_prefix, app)
            return root
        else:
            return app

    def mount(self, prefix):
        self.mount_prefix = prefix
        return self

    def set_config(self, config_instance):
        self.config = config_instance
        return self

    def add_search_engine(self, name, engine):
        if engine is None:
            self.search_engines.pop(name, None)
        self.search_engines[name] = engine
        return self

    def add_filter(self, name, filter):
        self.filters[name] = filter
        return self

    def add_routes(self, routes):
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
        self.app.install(create_injector(name, closure))
        return self

    def enable_cors(self):
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


class BottleAppFixScriptName(bottle.Bottle):
    def __call__(self, env, start):
        script_name = env.get('HTTP_DOSSIER_SCRIPT_NAME')
        if script_name is not None:
            env['SCRIPT_NAME'] = script_name
        return super(BottleAppFixScriptName, self).__call__(env, start)


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
