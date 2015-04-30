'''dossier.web.run

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

This is the "main" function of ``dossier.web``. Generally, you won't use
it directly, but instead import ``get_application`` from ``dossier.web``
and run your web application from your script.
'''

from __future__ import absolute_import, division, print_function

import argparse
import sys

import bottle

import dblogger
import kvlayer
import yakonfig
import yakonfig.factory

from dossier.web import config, search_engines as builtin_engines
from dossier.web.filter_preds import already_labeled
from dossier.web.folder import Folders
from dossier.web.routes import BottleAppFixScriptName, app


def get_application(routes=None, search_engines=None,
                    filter_preds=None, more_configurables=None,
                    label_hooks=None,
                    dbid_to_visid=lambda x: x,
                    visid_to_dbid=lambda x: x):
    '''Build and return a Bottle WSGI compatible application.

    The application returned is a ``Bottle`` app, which means you run it
    using Bottle's ``run`` method. Alternatively, you could name the
    returned app as ``application``, and it would runnable from any
    WSGI web server (e.g., ``gunicorn`` or ``uwsgi``).

    There are several optional parameters that let you inject your own
    routes, search engines and rank filters into :mod:`dossier.web`.

    In addition to `routes`, this will also import routes defined in the
    `external_routes` list in the `dossier.web` config block.

    :param routes: Your own Bottle routes.
    :type routes: ``list`` of :class:`dossier.web.Route`
    :param search_engines: Your own search engines. If not present, a single
                           ``random`` search engine will be available.
    :type search_engines: ``dict`` of ``name`` to
                          :class:`dossier.web.SearchEngine`
    :param filter_preds: Your own filter predicates. If not present, a single
                         ``already_labeled`` filter is always applied.
    :type filter_preds: ``dict`` of ``name`` to :class:`dossier.web.Filter`.
    :rtype: (:class:`argparse.Namespace`, :class:`bottle.Bottle`)
    '''
    routes = routes or []
    more_configurables = more_configurables or []
    label_hooks = label_hooks or []

    p = argparse.ArgumentParser(
        description='Run DossierStack web services.')
    p.add_argument('--bottle-debug', action='store_true',
                   help='Enable Bottle\'s debug mode.')
    p.add_argument('--reload', action='store_true',
                   help='Enable Bottle\'s reloading functionality.')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--host', default='localhost')
    p.add_argument('--server', default='wsgiref',
                   help='The web server to use. You only need to change this '
                        'if you\'re running a production server.')
    p.add_argument('--show-available-servers', action='store_true',
                   help='Shows list of available web server names and quits.')

    web_conf = config.Config()
    args = yakonfig.parse_args(p, [web_conf, dblogger, kvlayer, yakonfig] +
                               more_configurables)

    if args.show_available_servers:
        for name in sorted(bottle.server_names):
            try:
                __import__(name)
                print(name)
            except:
                pass
        sys.exit(0)

    configure_app(web_conf, search_engines, filter_preds, label_hooks,
                  dbid_to_visid, visid_to_dbid)

    for add_routes in routes:
        add_routes(app)

    if web_conf.config.get('url_prefix') is not None:
        root_app = BottleAppFixScriptName()
        root_app.mount(web_conf.config['url_prefix'], app)
        return args, root_app
    else:
        return args, app


# note that `app` is global from dossier.web.routes
def configure_app(web_conf=None, search_engines=None,
                  filter_preds=None, label_hooks=None,
                  dbid_to_visid=lambda x: x, visid_to_dbid=lambda x: x):
    if web_conf is None:
        web_conf = config.Config()
    search_engines = search_engines or {
        'random': builtin_engines.random,
        'plain_index_scan': builtin_engines.plain_index_scan,
    }
    filter_preds = filter_preds or {'already_labeled': already_labeled}

    app.install(config.JsonPlugin())
    app.install(
        config.create_injector('config', lambda: web_conf))
    app.install(
        config.create_injector('kvlclient', lambda: web_conf.kvlclient))
    app.install(
        config.create_injector('store', lambda: web_conf.store))
    app.install(
        config.create_injector('label_store', lambda: web_conf.label_store))
    app.install(
        config.create_injector('folders', lambda: web_conf.folders))
    app.install(
        config.create_injector('search_engines', lambda: search_engines))
    app.install(
        config.create_injector('label_hooks', lambda: label_hooks))
    app.install(
        config.create_injector('filter_preds', lambda: filter_preds))
    app.install(
        config.create_injector('request', lambda: bottle.request))
    app.install(
        config.create_injector('response', lambda: bottle.response))
    app.install(
        config.create_injector('dbid_to_visid', lambda: dbid_to_visid))
    app.install(
        config.create_injector('visid_to_dbid', lambda: visid_to_dbid))

    @app.hook('after_request')
    def enable_cors():
        bottle.response.headers['Access-Control-Allow-Origin'] = '*'
        bottle.response.headers['Access-Control-Allow-Methods'] = \
            'GET, POST, PUT, DELETE, OPTIONS'
        bottle.response.headers['Access-Control-Allow-Headers'] = \
            'Origin, X-Requested-With, Content-Type, Accept, Authorization'

    @app.error(405)
    def method_not_allowed(res):
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

    for extroute in web_conf.config.get('external_routes', []):
        mod, fun_name = extroute.split(':')
        fun = getattr(__import__(mod, fromlist=[fun_name]), fun_name)
        fun(app)

    return app


def run_with_argv(args, app):
    if args.server == 'gevent':
        import gevent.monkey
        gevent.monkey.patch_all()

    app.run(server=args.server, host=args.host, port=args.port,
            debug=args.bottle_debug, reloader=args.reload)


def main():
    run_with_argv(*get_application())


if __name__ == '__main__':
    main()
