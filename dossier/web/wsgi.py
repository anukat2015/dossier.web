'''dossier.web.wsgi

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

This is a WSGI compatible Python script that can be used to make
``dossier.web`` run with WSGI servers like ``gunicorn`` and ``uwsgi``.
'''

from dossier.web.run import get_application
_, application = get_application()
