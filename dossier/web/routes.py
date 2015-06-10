'''
Web service for active learning
===============================

dossier.web.routes is a REST stateful web service that can
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
.. autofunction:: v1_label_direct
.. autofunction:: v1_label_connected
.. autofunction:: v1_label_expanded
.. autofunction:: v1_label_negative_inference


Managing folders and sub-folders
================================

In many places where active learning is used, it can be useful to
provide the user with a means to group and categorize topics. In an
active learning setting, it is essential that we try to capture a
user's grouping of topics so that it can be used for ground truth data.
To that end, ``dossier.web`` exposes a set of web service endpoints
for managing folders and subfolders for a particular user. Folders and
subfolders are stored and managed by :mod:`dossier.label`, which means
they are automatically available as ground truth data.

The actual definition of what a folder or subfolder is depends on the
task the user is trying to perform. We tend to think of a folder as a
general topic and a subfolder as a more specific topic or "subtopic."
For example, a topic might be "cars" and some subtopics might be
"dealerships with cars I want to buy" or "electric cars."

The following end points allow one to add or list folders and
subfolders. There is also an endpoint for listing all of the items
in a single subfolder, where each item is a pair of ``(content_id,
subtopic_id)``.

In general, the identifier of a folder/subfolder is also used as its
name, similar to how identifiers in Wikipedia work. For example, if
a folder has a name "My Cars", then its identifier is ``My_Cars``.
More specifically, given any folder name ``NAME``, its corresponding
identifier can be obtained with ``NAME.replace(' ', '_')``.

All web routes accept and return *identifiers* (so space characters are
disallowed).

(Currently, there is no simple way to modify the name of an existing
folder or sub-folder. There is also no simple way to delete an existing
folder or sub-folder.)

.. autofunction:: v1_folder_list
.. autofunction:: v1_folder_add
.. autofunction:: v1_subfolder_list
.. autofunction:: v1_subfolder_add
.. autofunction:: v1_subtopic_list
'''
from __future__ import absolute_import, division, print_function
from functools import partial
from itertools import groupby, imap, ifilter, islice
import json
import logging
from operator import attrgetter
import os
import os.path as path
import urllib
import urlparse

import bottle

from dossier.fc import FeatureCollection
from dossier.label import Label, CorefValue
from dossier.label.run import label_to_dict
from dossier.web.folder import Folders
from dossier.web.search_engines import streaming_sample
from dossier.web import util
import yakonfig


app = bottle.Bottle()
logger = logging.getLogger(__name__)
web_static_path = path.join(path.split(__file__)[0], 'static')


@app.get('/dossier/v1/static/<name:path>')
def v1_static(name):
    return bottle.static_file(name, root=path.join(web_static_path, 'v1'))


@app.get('/dossier/v1/feature-collection/<cid>/search/<engine_name>')
def v1_search(request, response, visid_to_dbid, config,
              search_engines, filters, cid, engine_name):
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
    db_cid = visid_to_dbid(cid)
    try:
        search_engine = search_engines[engine_name]
    except KeyError as e:
        bottle.abort(404, 'Search engine "%s" does not exist.' % e.message)
    search_engine = (config.create(search_engine)
                           .set_query_id(db_cid)
                           .set_query_params(request.query))
    for name, filter in filters.items():
        search_engine.add_filter(name, config.create(filter))
    return search_engine.respond(response)


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
    return sorted(search_engines.keys())


@app.get('/dossier/v1/feature-collection/<cid>', json=True)
def v1_fc_get(visid_to_dbid, store, cid):
    '''Retrieve a single feature collection.

    The route for this endpoint is:
    ``/dossier/v1/feature-collections/<content_id>``.

    This endpoint returns a JSON serialization of the feature collection
    identified by ``content_id``.
    '''
    fc = store.get(visid_to_dbid(cid))
    if fc is None:
        bottle.abort(404, 'Feature collection "%s" does not exist.' % cid)
    return util.fc_to_json(fc)


@app.put('/dossier/v1/feature-collection/<cid>')
def v1_fc_put(request, response, visid_to_dbid, store, cid):
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
    store.put([(visid_to_dbid(cid), fc)])
    response.status = 201


@app.get('/dossier/v1/random/feature-collection', json=True)
def v1_random_fc_get(response, dbid_to_visid, store):
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
    return [dbid_to_visid(sample[0]), util.fc_to_json(store.get(sample[0]))]


@app.put('/dossier/v1/label/<cid1>/<cid2>/<annotator_id>')
def v1_label_put(request, response, visid_to_dbid, config, label_hooks,
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

    Optionally, the query parameters ``subtopic_id1`` and
    ``subtopic_id2`` may be specified. Neither, both or either may
    be given. ``subtopic_id1`` corresponds to a subtopic in
    ``content_id1`` and ``subtopic_id2`` corresponds to a subtopic
    in ``content_id2``.

    This endpoint returns status ``201`` upon successful storage.
    Any existing labels with the given ids are overwritten.

    After the label is stored, any label hooks passed via ``label_hooks``
    are executed. Each label hook should satisfy
    ``yakonfig.AutoFactory``. Once created, the label hook should be
    a function of one parameter: a :class:`dossier.label.Label`.
    '''
    coref_value = CorefValue(int(request.body.read()))
    lab = Label(visid_to_dbid(cid1), visid_to_dbid(cid2),
                annotator_id, coref_value,
                subtopic_id1=request.query.get('subtopic_id1'),
                subtopic_id2=request.query.get('subtopic_id2'))
    label_store.put(lab)

    # Run our hooks
    for label_hook_configurable in label_hooks:
        label_hook = config.create(label_hook_configurable)
        label_hook(lab)

    response.status = 201


@app.get('/dossier/v1/label/<cid>/direct', json=True)
@app.get('/dossier/v1/label/<cid>/subtopic/<subid>/direct', json=True)
def v1_label_direct(request, response, visid_to_dbid, dbid_to_visid,
                    label_store, cid, subid=None):
    '''Return directly connected labels.

    The routes for this endpoint are
    ``/dossier/v1/label/<cid>/direct`` and
    ``/dossier/v1/label/<cid>/subtopic/<subid>/direct``.

    This returns all directly connected labels for ``cid``. Or, if
    a subtopic id is given, then only directly connected labels for
    ``(cid, subid)`` are returned.

    The data returned is a JSON list of labels. Each label is a
    dictionary with the following keys: ``content_id1``,
    ``content_id2``, ``subtopic_id1``, ``subtopic_id2``,
    ``annotator_id``, ``epoch_ticks`` and ``value``.
    '''
    lab_to_json = partial(label_to_json, dbid_to_visid)
    ident = make_ident(visid_to_dbid(cid), subid)
    labs = imap(lab_to_json, label_store.directly_connected(ident))
    return list(paginate(request, response, labs))


@app.get('/dossier/v1/label/<cid>/connected', json=True)
@app.get('/dossier/v1/label/<cid>/subtopic/<subid>/connected', json=True)
def v1_label_connected(request, response, visid_to_dbid, dbid_to_visid,
                       label_store, cid, subid=None):
    '''Return a connected component of positive labels.

    The routes for this endpoint are
    ``/dossier/v1/label/<cid>/connected`` and
    ``/dossier/v1/label/<cid>/subtopic/<subid>/connected``.

    This returns the edges for the connected component of
    either ``cid`` or ``(cid, subid)`` if a subtopic identifier
    is given.

    The data returned is a JSON list of labels. Each label is a
    dictionary with the following keys: ``content_id1``,
    ``content_id2``, ``subtopic_id1``, ``subtopic_id2``,
    ``annotator_id``, ``epoch_ticks`` and ``value``.
    '''
    lab_to_json = partial(label_to_json, dbid_to_visid)
    ident = make_ident(visid_to_dbid(cid), subid)
    labs = imap(lab_to_json, label_store.connected_component(ident))
    return list(paginate(request, response, labs))


@app.get('/dossier/v1/label/<cid>/expanded', json=True)
@app.get('/dossier/v1/label/<cid>/subtopic/<subid>/expanded', json=True)
def v1_label_expanded(request, response, label_store,
                      visid_to_dbid, dbid_to_visid, cid, subid=None):
    '''Return an expansion of the connected component of positive labels.

    The routes for this endpoint are
    ``/dossier/v1/label/<cid>/expanded`` and
    ``/dossier/v1/label/<cid>/subtopic/<subid>/expanded``.

    This returns the edges for the expansion of the connected component
    of either ``cid`` or ``(cid, subid)`` if a subtopic identifier is
    given. Note that the expansion of a set of labels does not provide
    any new information content over a connected component. It is
    provided as a convenience for clients that want all possible labels
    in a connected component, regardless of whether one explicitly
    exists or not.

    The data returned is a JSON list of labels. Each label is a
    dictionary with the following keys: ``content_id1``,
    ``content_id2``, ``subtopic_id1``, ``subtopic_id2``,
    ``annotator_id``, ``epoch_ticks`` and ``value``.
    '''
    lab_to_json = partial(label_to_json, dbid_to_visid)
    ident = make_ident(visid_to_dbid(cid), subid)
    labs = imap(lab_to_json, label_store.connected_component(ident))
    return list(paginate(request, response, labs))


@app.get('/dossier/v1/label/<cid>/negative-inference', json=True)
def v1_label_negative_inference(request, response,
                                visid_to_dbid, dbid_to_visid,
                                label_store, cid):
    '''Return inferred negative labels.

    The route for this endpoint is:
    ``/dossier/v1/label/<cid>/negative-inference``.

    Negative labels are inferred by first getting all other content ids
    connected to ``cid`` through a negative label. For each directly
    adjacent ``cid'``, the connected components of ``cid`` and
    ``cid'`` are traversed to find negative labels.

    The data returned is a JSON list of labels. Each label is a
    dictionary with the following keys: ``content_id1``,
    ``content_id2``, ``subtopic_id1``, ``subtopic_id2``,
    ``annotator_id``, ``epoch_ticks`` and ``value``.
    '''
    # No subtopics yet? :-(
    lab_to_json = partial(label_to_json, dbid_to_visid)
    labs = imap(lab_to_json,
                label_store.negative_inference(visid_to_dbid(cid)))
    return list(paginate(request, response, labs))


@app.get('/dossier/v1/folder', json=True)
def v1_folder_list(request, kvlclient):
    '''Retrieves a list of folders for the current user.

    The route for this endpoint is: ``GET /dossier/v1/folder``.

    (Temporarily, the "current user" can be set via the
    ``annotator_id`` query parameter.)

    The payload returned is a list of folder identifiers.
    '''
    return sorted(imap(attrgetter('name'),
                       ifilter(lambda it: it.is_folder(),
                               new_folders(kvlclient, request).list('/'))))


@app.put('/dossier/v1/folder/<fid>')
def v1_folder_add(request, response, kvlclient, fid):
    '''Adds a folder belonging to the current user.

    The route for this endpoint is: ``PUT /dossier/v1/folder/<fid>``.

    If the folder was added successfully, ``201`` status is returned.

    (Temporarily, the "current user" can be set via the
    ``annotator_id`` query parameter.)
    '''
    fid = urllib.unquote(fid)
    new_folders(kvlclient, request).put_folder(fid)
    response.status = 201


@app.get('/dossier/v1/folder/<fid>/subfolder', json=True)
def v1_subfolder_list(request, response, kvlclient, fid):
    '''Retrieves a list of subfolders in a folder for the current user.

    The route for this endpoint is:
    ``GET /dossier/v1/folder/<fid>/subfolder``.

    (Temporarily, the "current user" can be set via the
    ``annotator_id`` query parameter.)

    The payload returned is a list of subfolder identifiers.
    '''
    fid = urllib.unquote(fid)
    try:
        return sorted(imap(attrgetter('name'),
                           ifilter(lambda it: it.is_folder(),
                                   new_folders(kvlclient, request).list(fid))))
    except KeyError:
        response.status = 404
        return []


@app.put('/dossier/v1/folder/<fid>/subfolder/<sfid>/<cid>')
@app.put('/dossier/v1/folder/<fid>/subfolder/<sfid>/<cid>/<subid>')
def v1_subfolder_add(request, response, kvlclient,
                     fid, sfid, cid, subid=None):
    '''Adds a subtopic to a subfolder for the current user.

    The route for this endpoint is:
    ``PUT /dossier/v1/folder/<fid>/subfolder/<sfid>/<cid>/<subid>``.

    ``fid`` is the folder identifier, e.g., ``My_Folder``.

    ``sfid`` is the subfolder identifier, e.g., ``My_Subtopic``.

    ``cid`` and ``subid`` are the content id and subtopic id of the
    subtopic being added to the subfolder.

    If the subfolder does not already exist, it is created
    automatically. N.B. An empty subfolder cannot exist!

    If the subtopic was added successfully, ``201`` status is returned.

    (Temporarily, the "current user" can be set via the
    ``annotator_id`` query parameter.)
    '''
    if subid is not None:
        assert '@' not in subid
    path = [
        urllib.unquote(fid),
        urllib.unquote(sfid),
        cid + (('@' + subid) if subid is not None else ''),
    ]
    path = '/'.join(path)
    new_folders(kvlclient, request).put(path)
    response.status = 201


@app.get('/dossier/v1/folder/<fid>/subfolder/<sfid>', json=True)
def v1_subtopic_list(request, response, kvlclient, fid, sfid):
    '''Retrieves a list of items in a subfolder.

    The route for this endpoint is:
    ``GET /dossier/v1/folder/<fid>/subfolder/<sfid>``.

    (Temporarily, the "current user" can be set via the
    ``annotator_id`` query parameter.)

    The payload returned is a list of two element arrays. The first
    element in the array is the item's content id and the second
    element is the item's subtopic id.
    '''
    path = urllib.unquote(fid) + '/' + urllib.unquote(sfid)
    try:
        items = []
        for it in new_folders(kvlclient, request).list(path):
            if '@' in it.name:
                items.append(it.name.split('@'))
            else:
                items.append((it.name, None))
        return items
    except KeyError:
        response.status = 404
        return []


@app.delete('/dossier/v1/folder/<fid>')
@app.delete('/dossier/v1/folder/<fid>/subfolder/<sfid>')
@app.delete('/dossier/v1/folder/<fid>/subfolder/<sfid>/<cid>')
@app.delete('/dossier/v1/folder/<fid>/subfolder/<sfid>/<cid>/<subid>')
def v1_folder_delete(request, response, kvlclient,
                     fid, sfid=None, cid=None, subid=None):
    new_folders(kvlclient, request).delete(make_path(fid, sfid, cid, subid))
    response.status = 200


@app.post('/dossier/v1/folder/<fid_src>/rename/<fid_dest>')
@app.post('/dossier/v1/folder/<fid_src>/subfolder/<sfid_src>/rename/<fid_dest>/subfolder/<sfid_dest>')  # noqa
def v1_folder_rename(request, response, kvlclient,
                     fid_src, fid_dest, sfid_src=None, sfid_dest=None):
    src, dest = make_path(fid_src, sfid_src), make_path(fid_dest, sfid_dest)
    new_folders(kvlclient, request).move(src, dest)
    response.status = 200


if os.getenv('DOSSIER_WEB_DEV', '0') == '1':
    @app.delete('/dossier/v1/delete-all-labels')
    def v1_delete_all_labels(response, store, label_store):
        label_store.delete_all()
        response.status = 204

    @app.delete('/dossier/v1/delete-all-fcs')
    def v1_delete_all_fcs(response, store):
        store.delete_all()
        response.status = 204

    @app.delete('/dossier/v1/delete-all-folders')
    def v1_delete_all_folders(request, response, kvlclient):
        new_folders(kvlclient, request).delete_everything()
        response.status = 204

    @app.delete('/dossier/v1/delete-everything')
    def v1_delete_everything(response, kvlclient):
        kvlclient.delete_namespace()
        response.status = 204


def make_path(fid, sfid=None, cid=None, subid=None):
    path = [urllib.unquote(fid)]
    if sfid is not None:
        path.append(urllib.unquote(sfid))
        if cid is not None:
            if subid is not None:
                path.append(cid + '@' + subid)
            else:
                path.append(cid)
    return '/'.join(path)


def folder_id_to_name(ident):
    return ident.replace('_', ' ')


def folder_name_to_id(name):
    return name.replace(' ', '_')


def assert_valid_folder_id(ident):
    if ' ' in ident or '/' in ident:
        bottle.abort(
            500, "Folder ids cannot contain spaces or '/' characters.")


def get_annotator_id(request):
    # TODO: Get the annotator id from REMOTE_USER environment variable?
    # Currently, we're just accepting anything in the query parameter
    # `annotator_id` (and assuming `unknown` if absent). ---AG
    return request.query.get('annotator_id', 'unknown')


def str_to_max_int(s, maximum):
    try:
        return min(maximum, int(s))
    except (ValueError, TypeError):
        return maximum


def make_ident(content_id, subtopic_id):
    if subtopic_id is None:
        return content_id
    else:
        return (content_id, subtopic_id)


def label_to_json(dbid_to_visid, lab):
    d = label_to_dict(lab)
    d['content_id1'] = dbid_to_visid(d['content_id1'])
    d['content_id2'] = dbid_to_visid(d['content_id2'])
    return d


def paginate(request, response, it):
    def setqp(param, val):
        return set_query_param(request.url, param, val)

    def tuple_to_link((rel, url)):
        return '<%s>; rel="%s"' % (url, rel)

    def add_link_headers(page, per):
        links = []
        # Add the "first" and "prev" links.
        if page > 1:
            links.append(('first', setqp('page', '1')))
            links.append(('prev', setqp('page', str(page - 1))))
        # We never really know when the stream ends, so there is always a
        # "next" link.
        links.append(('next', setqp('page', str(page + 1))))
        response.headers['Link'] = ', '.join(map(tuple_to_link, links))

    page = max(1, int(request.query.get('page', 1)))
    per = min(500, max(1, int(request.query.get('perpage', 2))))
    start = (page - 1) * per
    end = start + per
    add_link_headers(page, per)
    return islice(it, start, end)


def set_query_param(url, param, value):
    '''Returns a new URL with the given query parameter set to ``value``.

    ``value`` may be a list.'''
    scheme, netloc, path, qs, frag = urlparse.urlsplit(url)
    params = urlparse.parse_qs(qs)
    params[param] = value
    qs = urllib.urlencode(params, doseq=True)
    return urlparse.urlunsplit((scheme, netloc, path, qs, frag))


def dedup(it):
    '''Dedups a sorted iterable in constant memory.'''
    for _, group in groupby(it):
        for lab in group:
            yield lab
            break


def new_folders(kvlclient, request):
    try:
        config = yakonfig.get_global_config('dossier.folders')
        # For old configs.
        if 'prefix' in config:
            config['namespace'] = config.pop('prefix')
    except KeyError:
        config = {}
    if 'annotator_id' in request.query:
        config['owner'] = request.query['annotator_id']
    return Folders(kvlclient, **config)
