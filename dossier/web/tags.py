'''
Tagging for Dossier Stack
=========================

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

This module provides a Python class and a REST API to a *very simple*
hierarchical tagging service with support for fast auto-completion.

There are three flavors of routes: associate a tag with an object in
an HTML document, list tags given a parent tag and list associations
given some attribute (e.g., a tag or a stream id or a URL).

.. autofunction:: v1_tag_associate
.. autofunction:: v1_tag_list
.. autofunction:: v1_tag_suggest
.. autofunction:: v1_stream_id_associations
.. autofunction:: v1_url_associations
.. autofunction:: v1_tag_associations
'''
from __future__ import absolute_import, division, print_function

import json
import logging
import urllib

import bottle
from elasticsearch import ConflictError, Elasticsearch, TransportError
import yakonfig


logger = logging.getLogger(__name__)
app = bottle.Bottle()


@app.post('/dossier/v1/tags/associations/tag/<tag:path>')
def v1_tag_associate(request, tags, tag):
    '''Associate an HTML element with a tag.

    The association should be a JSON serialized object on the
    request body. Here is an example association that should
    make the object's structure clear:

    .. code-block:: python

        {
            "url": "http://example.com/abc/xyz?foo=bar",
            "text": "The text the user highlighted.",
            "stream_id": "{unix timestamp}-{md5 of url}",
            "hash": "{nilsimsa hash of the HTML}",
            "timestamp": {unix timestamp},
            "xpath": {
                "start_node": "/html/body/p[1]/text()[2]",
                "start_idx": 3,
                "end_node": "/html/body/p[1]/text()[3]",
                "end_idx": 9
            }
        }

    All fields are required and cannot be empty or ``null``.

    The tag of the association should be specified in the URL
    and is delimited by ``//``.
    '''
    tag = tag.decode('utf-8').strip()
    assoc = dict(json.loads(request.body.read()), **{'tag': tag})
    tags.add(assoc)


@app.get('/dossier/v1/tags/list')
@app.get('/dossier/v1/tags/list/<tag:path>')
def v1_tag_list(tags, tag=''):
    '''List all direct children tags of the given parent.

    If no parent is specified, then list all top-level tags.

    The JSON returned for ``/dossier/v1/tags/list/foo/bar``
    might look like this:

    .. code-block:: python

        {
            'children': [
                {'name': 'baz', 'parent': 'bar', 'tag': 'foo/bar/baz'},
            ]
        }
    '''
    tag = tag.decode('utf-8').strip()
    return {'children': tags.list(tag)}


@app.get('/dossier/v1/tags/suggest/prefix/<prefix>')
@app.get('/dossier/v1/tags/suggest/prefix/<prefix>/parent/<parent:path>')
def v1_tag_suggest(request, tags, prefix, parent=''):
    '''Provide fast suggestions for tag components.

    This yields suggestions for *components* of a tag and a given
    prefix. For example, given the tags ``foo/bar/baz`` and
    ``fob/bob``, here are some example completions (ordering may be
    different):

    .. code-block:: text

        /dossier/v1/tags/suggest/prefix/f => ['foo', 'fob']
        /dossier/v1/tags/suggest/prefix/foo => ['foo']
        /dossier/v1/tags/suggest/prefix/b/parent/foo => ['bar']
        /dossier/v1/tags/suggest/prefix/b/parent/fob => ['bob']
        /dossier/v1/tags/suggest/prefix/b/parent/foo/bar => ['baz']

    N.B. Each of the lists above are wrapped in the following
    JSON envelope for the response:

    .. code-block:: text

        {'suggestions': ['foo', 'fob']}

    An optional query parameter, ``limit``, may be passed to control
    the number of suggestions returned.
    '''
    prefix = prefix.decode('utf-8').strip()
    parent = parent.decode('utf-8').strip()
    limit = min(10000, int(request.params.get('limit', 100)))
    return {'suggestions': tags.suggest(parent, prefix, limit=limit)}


@app.get('/dossier/v1/tags/associations/stream_id/<stream_id:path>')
def v1_stream_id_associations(tags, stream_id):
    '''Retrieve associations for a given stream_id.

    The associations returned have the exact same structure as defined
    in the ``v1_tag_associate`` route with one addition: a ``tag``
    field contains the full tag name for the association.
    '''
    stream_id = stream_id.decode('utf-8').strip()
    return {'associations': tags.assocs_by_stream_id(stream_id)}


@app.get('/dossier/v1/tags/associations/url/<url:path>')
def v1_url_associations(tags, url):
    '''Retrieve associations for a given URL.

    The associations returned have the exact same structure as defined
    in the ``v1_tag_associate`` route with one addition: a ``tag``
    field contains the full tag name for the association.
    '''
    url = urllib.unquote_plus(url.decode('utf-8')).strip()
    return {'associations': tags.assocs_by_url(url)}


@app.get('/dossier/v1/tags/associations/tag/<tag:path>')
def v1_tag_associations(tags, tag):
    '''Retrieve associations for a given tag.

    The associations returned have the exact same structure as defined
    in the ``v1_tag_associate`` route with one addition: a ``tag``
    field contains the full tag name for the association.
    '''
    tag = tag.decode('utf-8').strip()
    return {'associations': tags.assocs_by_tag(tag)}


class Tags(object):
    config_name = 'dossier.tags'

    @classmethod
    def configured(cls):
        return cls(**yakonfig.get_global_config('dossier.tags'))

    def __init__(self, hosts=None, namespace=None, type_prefix='',
                 shards=10, replicas=0, tag_delimiter='/'):
        if hosts is None:
            raise yakonfig.ProgrammerError(
                'Tags needs at least one host specified.')
        if namespace is None:
            raise yakonfig.ProgrammerError('Tags needs a namespace defined.')
        self.conn = Elasticsearch(hosts=hosts, timeout=60, request_timeout=60)
        self.index = 'tags_%s' % namespace
        self.type_tag = '%stag' % type_prefix
        self.type_assoc = '%sassociation' % type_prefix
        self.shards = shards
        self.replicas = replicas
        self.delim = tag_delimiter

        created1 = self._create_index()
        created2 = self._create_mappings()
        if created1 or created2:
            # It is possible to create an index and quickly launch a request
            # that will fail because the index hasn't been set up yet. Usually,
            # you'll get a "no active shards available" error.
            #
            # Since index creation is a very rare operation (it only happens
            # when the index doesn't already exist), we sit and wait for the
            # cluster to become healthy.
            self.conn.cluster.health(index=self.index,
                                     wait_for_status='yellow')

    def add(self, assoc):
        self._validate_association(assoc)
        tag = self._normalize_tag(assoc['tag'])
        if len(tag) == 0:
            return
        self.conn.create(
            index=self.index, doc_type=self.type_assoc, body=assoc)

        # Start with creating the full tag and continue creating parent tags
        # until one exists or until we hit root. This lets us save some
        # round trips in the common case (the tag is already created).
        parts = tag.split(self.delim)
        while len(parts) > 0:
            tag = self.delim.join(parts)
            doc_tag = {
                'tag': tag,
                'parent': self.delim.join(parts[:-1]),
                'name': parts[-1],
            }
            try:
                self.conn.create(index=self.index, doc_type=self.type_tag,
                                 id=tag, body=doc_tag)
            except ConflictError as e:
                # Yay for brittle substring search for error detection!
                if 'DocumentAlreadyExistsException' in e.error:
                    break
                raise
            parts = parts[:-1]

    def list(self, parent_tag):
        parent_tag = self._normalize_tag(parent_tag)
        return self._term_query(self.type_tag, 'parent', parent_tag)

    def suggest(self, parent, prefix, limit=100):
        if prefix == '':
            # No sense in issuing a request when we already know the answer.
            return []
        body = {
            'tag': {
                'text': prefix,
                'completion': {
                    'field': 'name.suggest',
                    'size': limit,
                    'context': {
                        'parent': parent,
                    },
                },
            },
        }
        hits = self.conn.suggest(index=self.index, body=body)
        if 'tag' not in hits:
            return []
        return map(lambda hit: hit['text'], hits['tag'][0]['options'])

    def assocs_by_tag(self, tag):
        tag = self._normalize_tag(tag)
        return self._term_query(self.type_assoc, 'tag', tag)

    def assocs_by_url(self, url):
        return self._term_query(self.type_assoc, 'url', url)

    def assocs_by_stream_id(self, stream_id):
        return self._term_query(self.type_assoc, 'stream_id', stream_id)

    def sync(self):
        '''Tells ES to tell Lucene to do an fsync.

        This guarantees that any previous calls to ``add`` will be
        flushed to disk and available in subsequent searches.

        Generally, this should only be used in test code.
        '''
        self.conn.indices.refresh(index=self.index)

    def delete_all(self):
        '''Deletes all tag data.

        This does not destroy the ES index, but instead only
        deletes all tags with the configured doc types.
        '''
        try:
            self.conn.indices.delete_mapping(
                index=self.index, doc_type=self.type_tag)
        except TransportError:
            logger.warn('type %r in index %r already deleted',
                        self.index, self.type_tag, exc_info=True)
        try:
            self.conn.indices.delete_mapping(
                index=self.index, doc_type=self.type_assoc)
        except TransportError:
            logger.warn('type %r in index %r already deleted',
                        self.index, self.type_assoc, exc_info=True)

    def _create_index(self):
        'Create the index'
        # This can race, but that should be OK.
        # Worst case, we initialize with the same settings more than
        # once.
        if self.conn.indices.exists(index=self.index):
            return False
        try:
            self.conn.indices.create(
                index=self.index, timeout=60, request_timeout=60, body={
                    'settings': {
                        'number_of_shards': self.shards,
                        'number_of_replicas': self.replicas,
                    },
                })
        except TransportError:
            # Hope that this is an "index already exists" error...
            logger.warn('index already exists? OK', exc_info=True)
        return True

    def _create_mappings(self):
        'Create the field type mapping.'
        created1 = self._create_tag_mapping()
        created2 = self._create_assoc_mapping()
        return created1 or created2

    def _create_tag_mapping(self):
        mapping = self.conn.indices.get_mapping(
            index=self.index, doc_type=self.type_tag)
        if len(mapping) > 0:
            return False
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type_tag,
            timeout=60, request_timeout=60,
            body={
                self.type_tag: {
                    'dynamic': False,
                    'properties': {
                        'parent': {
                            'type': 'string',
                            'index': 'not_analyzed',
                        },
                        'name': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'fields': {
                                'suggest': {
                                    'type': 'completion',
                                    'index_analyzer': 'simple',
                                    'search_analyzer': 'simple',
                                    'payloads': False,
                                    'preserve_separators': True,
                                    'preserve_position_increments': True,
                                    'max_input_length': 256,
                                    'context': {
                                        'parent': {
                                            'type': 'category',
                                            'path': 'parent',
                                        },
                                    },
                                },
                            },
                        },
                        'tag': {
                            'type': 'string',
                            'index': 'not_analyzed',
                        },
                    },
                },
            })
        return True

    def _create_assoc_mapping(self):
        mapping = self.conn.indices.get_mapping(
            index=self.index, doc_type=self.type_assoc)
        if len(mapping) > 0:
            return False
        self.conn.indices.put_mapping(
            index=self.index, doc_type=self.type_assoc,
            timeout=60, request_timeout=60,
            body={
                self.type_assoc: {
                    'dynamic': False,
                    'properties': {
                        'url': {'type': 'string', 'index': 'not_analyzed'},
                        'text': {'type': 'string', 'index': 'analyzed'},
                        'tag': {'type': 'string', 'index': 'not_analyzed'},
                        'stream_id': {'type': 'string',
                                      'index': 'not_analyzed'},
                        'hash': {'type': 'string', 'index': 'not_analyzed'},
                        'timestamp': {'type': 'integer',
                                      'index': 'not_analyzed'},
                        'xpath': {
                            'type': 'object',
                            'dynamic': False,
                            'properties': {
                                'start_node': {'type': 'string',
                                               'index': 'no'},
                                'start_idx': {'type': 'integer',
                                              'index': 'no'},
                                'end_node': {'type': 'string',
                                             'index': 'no'},
                                'end_idx': {'type': 'integer',
                                            'index': 'no'},
                            },
                        },
                    },
                },
            })
        return True

    def _validate_association(self, assoc):
        def check_field(d, (name, ty), prefix=''):
            if name in d and isinstance(d[name], ty):
                if ty is basestring and len(d[name]) > 0:
                    return
                elif d[name] is not None:
                    return
            raise ValueError('missing field: %s%s' % (prefix, name))

        # The correctness of this function is not required for preventing
        # threats. It exists purely as a way to provide good failure modes.
        required_fields = [
            ('url', basestring),
            ('text', basestring),
            ('tag', basestring),
            ('stream_id', basestring),
            ('hash', basestring),
            ('timestamp', int),
            ('xpath', dict),
        ]
        required_fields_xpath = [
            ('start_node', basestring),
            ('start_idx', int),
            ('end_node', basestring),
            ('end_idx', int),
        ]
        for field in required_fields:
            check_field(assoc, field)
        for field in required_fields_xpath:
            check_field(assoc['xpath'], field, prefix='xpath/')
        if len(assoc) > len(required_fields):
            raise ValueError('association object has too many fields')
        if len(assoc['xpath']) > len(required_fields_xpath):
            raise ValueError('association xpath object has too many fields')

    def _normalize_tag(self, tag):
        return self.delim.join(map(unicode.strip, tag.split(self.delim)))

    def _term_query(self, ty, field, value):
        query = {
            'query': {
                'constant_score': {
                    'filter': {
                        'term': {
                            field: value,
                        },
                    },
                },
            },
        }
        results = self.conn.search(index=self.index, doc_type=ty, body=query,
                                   size=1000)
        return map(lambda r: r['_source'], results['hits']['hits'])


class TagsSync(Tags):
    def add(self, assoc):
        super(TagsSync, self).add(assoc)
        self.sync()
