'''dossier.web.config

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
import functools
import inspect
import json
import logging
import threading
import traceback

import bottle

from dossier.label import LabelStore
from dossier.store import Store
import kvlayer
import yakonfig
import yakonfig.factory


logger = logging.getLogger(__name__)


def safe_service(attr, default_value=None):
    '''A **method** decorator for creating safe services.

    Given an attribute name, this returns a decorator for creating
    safe services. Namely, if a service that is not yet available is
    requested (like a database connection), then ``safe_service`` will
    log any errors and set the given attribute to ``default_value``.

    :param str attr: attribute name
    :param object default_value: default value to set
    :rtype: decorator
    '''
    def _(fun):
        @functools.wraps(fun)
        def run(self):
            try:
                return fun(self)
            except:
                logger.error(traceback.format_exc())
                setattr(self, attr, default_value)
        return run
    return _


def thread_local_property(name):
    '''Creates a thread local ``property``.'''
    name = '_thread_local_' + name
    def fget(self):
        try:
            return getattr(self, name).value
        except AttributeError:
            return None
    def fset(self, value):
        getattr(self, name).value = value
    return property(fget=fget, fset=fset)


class Config(yakonfig.factory.AutoFactory):
    '''Configuration for dossier.web.

    .. automethod:: dossier.web.Config.create
    .. autoattribute:: dossier.web.Config.kvlclient
    .. autoattribute:: dossier.web.Config.store
    .. autoattribute:: dossier.web.Config.label_store
    '''
    _THREAD_LOCALS = ['store', 'label_store', 'kvlclient']
    for n in _THREAD_LOCALS:
        locals()['_' + n] = thread_local_property(n)


    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self.new_config()

        # Create new thread local containers for values that cannot be used
        # simultaneously across threads.
        for n in self._THREAD_LOCALS:
            setattr(self, '_thread_local_' + n, threading.local())

    def new_config(self):
        super(Config, self).new_config()
        self._idx_map = None

    @property
    def config_name(self):
        return 'dossier.web'

    @property
    def web_config(self):
        return self

    @property
    def auto_config(self):
        return [Store, LabelStore]

    @property
    @safe_service('_store')
    def store(self):
        '''Return a thread local :class:`dossier.store.Store` client.'''
        if self._store is None:
            config = global_config('dossier.store')
            self._store = self.create(Store, config=config)
        return self._store

    @property
    @safe_service('_label_store')
    def label_store(self):
        '''Return a thread local :class:`dossier.label.LabelStore` client.'''
        if self._label_store is None:
            config = global_config('dossier.label')
            if 'kvlayer' in config:
                kvl = kvlayer.client(config=config['kvlayer'])
                self._label_store = LabelStore(kvl)
            else:
                self._label_store = self.create(LabelStore, config=config)
        return self._label_store

    @property
    @safe_service('_kvlclient')
    def kvlclient(self):
        '''Return a thread local ``kvlayer`` client.'''
        if self._kvlclient is None:
            self._kvlclient = kvlayer.client()
        return self._kvlclient


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


def global_config(name):
    try:
        return yakonfig.get_global_config(name)
    except KeyError:
        return {}
