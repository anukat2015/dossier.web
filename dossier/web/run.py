'''dossier.web.run

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

This is the "main" function of ``dossier.web``. Generally, you won't use
it directly, but instead import ``get_application`` from ``dossier.web``
and run your web application from your script.
'''

from __future__ import absolute_import, division, print_function

import argparse

import dblogger
import kvlayer
import yakonfig
import yakonfig.factory

from dossier.web.builder import WebBuilder, add_cli_arguments
from dossier.web.config import Config


def default_app():
    config = Config()
    p = argparse.ArgumentParser(description='Run DossierStack web services.')
    add_cli_arguments(p)
    args = yakonfig.parse_args(p, [config, dblogger, kvlayer, yakonfig])
    app = WebBuilder().set_config(config).enable_cors().get_app()
    return args, app


def main():
    args, app = default_app()
    app.run(server='wsgiref', host=args.host, port=args.port,
            debug=args.bottle_debug, reloader=args.reload)


if __name__ == '__main__':
    main()
