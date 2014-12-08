'''dossier.web.routes.app is a REST stateful web service that can
drive Dossier Stack's an active ranking models and user interface, as
well as other search technologies.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

There are only a few API end points. They provide searching, storage
and retrieval of feature collections along with storage of ground
truth data as labels. Labels are typically used in the implementation
of a search engine to filter or improve the recommendations returned.

The API end points are documented as functions in this module.

.. autofunction:: v1_search
.. autofunction:: v1_search_engines
.. autofunction:: v1_fc_get
.. autofunction:: v1_fc_put
.. autofunction:: v1_random_fc_get
.. autofunction:: v1_label_put

'''
from __future__ import absolute_import, division, print_function
import json
import logging
import os.path as path

import bottle

from dossier.fc import FeatureCollection, StringCounter
from dossier.label import Label, CorefValue
from dossier.web.search_engines import streaming_sample


app = bottle.Bottle()
logger = logging.getLogger(__name__)
web_static_path = path.join(path.split(__file__)[0], 'static')


@app.get('/dossier/v1/static/<name:path>')
def v1_static(name):
    return bottle.static_file(name, root=path.join(web_static_path, 'v1'))


@app.get('/dossier/v1/feature-collection/<cid>/search/<engine_name>', json=True)
def v1_search(request, config, search_engines, filter_preds, cid, engine_name):
    '''Search feature collections.

    The route for this endpoint is:
    ``/dossier/v1/<content_id>/search/<search_engine_name>``.

    ``content_id`` can be any *profile* content identifier. (This
    restriction may be lifted at some point.) Namely, it must start
    with ``p|``.

    ``engine_name`` corresponds to the search strategy to
    use. The list of available search engines can be retrieved with the
    :func:`v1_search_engines` endpoint.

    This endpoint returns a JSON payload which is an object with a
    single key, ``results``. ``results`` is a list of objects, where
    the objects each have ``content_id`` and ``fc`` attributes.
    ``content_id`` is the unique identifier for the result returned,
    and ``fc`` is a JSON serialization of a feature collection.

    There are also two query parameters:

    * **limit** limits the number of results to the number given.
    * **filter** sets the filtering function. The default
      filter function, ``already_labeled``, will filter out any
      feature collections that have already been labeled with the
      query ``content_id``.
    '''
    omit_fc = request.query.pop('omit_fc', '0') == '1'

    try:
        search_engine = search_engines[engine_name]
    except KeyError as e:
        bottle.abort(404,
            'Search engine "%s" does not exist.' % e.message)
    try:
        fil_name = request.query.pop('filter', 'already_labeled')
        init_filter_pred = None
        if len(fil_name) > 0:
            init_filter_pred = filter_preds[fil_name]
    except KeyError as e:
        bottle.abort(404,
            'Rank filter "%s" does not exist.' % e.message)
    search_engine = config.create(search_engine)

    filter_pred = None
    if init_filter_pred is not None:
        init_filter_pred = config.create(init_filter_pred)
        filter_pred = init_filter_pred(cid)
    else:
        filter_pred = lambda _: True

    kwargs = dict(request.query)
    kwargs['filter_pred'] = filter_pred
    kwargs['limit'] = str_to_max_int(request.query.get('limit'), 20)

    results = search_engine(cid, **kwargs)
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
        if not omit_fc:
            result['fc'] = fc_to_json(fc)
        transformed.append(result)
    results['results'] = transformed
    return results


@app.get('/dossier/v1/search_engines', json=True)
def v1_search_engines(search_engines):
    '''List available search engines.

    The route for this endpoint is: ``/dossier/v1/search_engines``.

    This endpoint returns a JSON payload which is an object with two
    keys: ``default`` and ``names``. ``default`` corresponds to a
    chosen default search engine. This value will *always* correspond
    to a valid search engine. ``names`` is an array of all available
    search engines (including ``default``).
    '''
    ## explain where search_engines comes from...
    return sorted(search_engines.keys())


@app.get('/dossier/v1/feature-collection/<cid>')
def v1_fc_get(store, cid):
    '''Retrieve a single feature collection.

    The route for this endpoint is:
    ``/dossier/v1/feature-collections/<content_id>``.

    This endpoint returns a JSON serialization of the feature collection
    identified by ``content_id``.
    '''
    fc = store.get(cid)
    if fc is None:
        bottle.abort(404, 'Feature collection "%s" does not exist.' % cid)
    return fc_to_json(fc)


@app.put('/dossier/v1/feature-collection/<cid>')
def v1_fc_put(request, response, store, cid):
    '''Store a single feature collection.

    The route for this endpoint is:
    ``PUT /dossier/v1/feature-collections/<content_id>``.

    ``content_id`` is the id to associate with the given feature
    collection. The feature collection should be in the request
    body serialized as JSON.

    This endpoint returns status ``201`` upon successful storage.
    An existing feature collection with id ``content_id`` is
    overwritten.
    '''
    fc = FeatureCollection.from_dict(json.load(request.body))
    store.put([(cid, fc)])
    response.status = 201


@app.get('/dossier/v1/random/feature-collection', json=True)
def v1_random_fc_get(response, store):
    '''Retrieves a random feature collection from the database.

    The route for this endpoint is:
    ``GET /dossier/v1/random/feature-collection``.

    Assuming the database has at least one feature collection,
    this end point returns an array of two elements. The first
    element is the content id and the second element is a
    feature collection (in the same format returned by
    :func:`dossier.web.routes.v1_fc_get`).

    If the database is empty, then a 404 error is returned.

    Note that currently, this may not be a uniformly random sample.
    '''
    # Careful, `store.scan()` would be obscenely slow here...
    sample = streaming_sample(store.scan_ids(), 1, 1000)
    if len(sample) == 0:
        bottle.abort(404, 'The feature collection store is empty.')
    return [sample[0], fc_to_json(store.get(sample[0]))]


@app.put('/dossier/v1/label/<cid1>/<cid2>/<annotator_id>')
def v1_label_put(request, response, config, label_hooks,
                 label_store, cid1, cid2, annotator_id):
    '''Store a single label.

    The route for this endpoint is:
    ``PUT /dossier/v1/labels/<content_id1>/<content_id2>/<annotator_id>``.

    ``content_id`` are the ids of the feature collections to
    associate. ``annotator_id`` is a string that identifies the
    human that created the label. The value of the label should
    be in the request body as one of the following three values:
    ``-1`` for not coreferent, ``0`` for "I don't know if they
    are coreferent" and ``1`` for coreferent.

    This endpoint returns status ``201`` upon successful storage.
    Any existing labels with the given ids are overwritten.

    After the label is stored, any label hooks passed via ``label_hooks``
    are executed.

    Currently, there is no way to *retrieve* labels through the
    API.
    '''
    coref_value = CorefValue(int(request.body.read()))
    lab = Label(cid1, cid2, annotator_id, coref_value)
    label_store.put(lab)

    # Run our hooks
    for label_hook_configurable in label_hooks:
        label_hook = config.create(label_hook_configurable)
        label_hook(lab)

    response.status = 201


def str_to_max_int(s, maximum):
    try:
        return min(maximum, int(s))
    except (ValueError, TypeError):
        return maximum


def fc_to_json(fc):
    d = {}
    for name, feat in fc.iteritems():
        if isinstance(feat, (unicode, StringCounter)):
            d[name] = feat
    return d
