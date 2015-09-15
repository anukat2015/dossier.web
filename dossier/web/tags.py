from __future__ import absolute_import, division, print_function

import json
import logging

import bottle
from elasticsearch import Elasticsearch, TransportError
import yakonfig


logger = logging.getLogger(__name__)
app = bottle.Bottle()


@app.get('/dossier/v1/tags/list')
@app.get('/dossier/v1/tags/list/<tag:path>')
def v1_tag_list(tags, tag=''):
    tag = tag.decode('utf-8').strip()
    return {'children': tags.list(tag)}


@app.get('/dossier/v1/tags/associations/url/<url:path>')
def v1_url_associations(tags, url):
    url = url.decode('utf-8').strip()
    return {'associations': tags.assocs_by_url(url)}


@app.get('/dossier/v1/tags/associations/tag/<tag:path>')
def v1_tag_associations(tags, tag):
    tag = tag.decode('utf-8').strip()
    return {'associations': tags.assocs_by_tag(tag)}


@app.post('/dossier/v1/tags/associations/tag/<tag:path>')
def v1_tag_associate(request, tags, tag):
    tag = tag.decode('utf-8').strip()
    assoc = dict(json.loads(request.body.read()), **{'tag': tag})
    tags.add(assoc)


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

        parts = []
        for part in tag.split(self.delim):
            parts.append(part)
            tag = self.delim.join(parts)
            doc_tag = {
                'tag': tag,
                'parent': self.delim.join(parts[:-1]),
                'name': part,
            }
            self.conn.create(
                index=self.index, doc_type=self.type_tag, id=tag, body=doc_tag)

    def list(self, parent_tag):
        parent_tag = self._normalize_tag(parent_tag)
        return self._term_query(self.type_tag, 'parent', parent_tag)

    def assocs_by_tag(self, tag):
        tag = self._normalize_tag(tag)
        return self._term_query(self.type_assoc, 'tag', tag)

    def assocs_by_url(self, url):
        return self._term_query(self.type_assoc, 'url', url)

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
                        'parent': {'type': 'string', 'index': 'not_analyzed'},
                        'name': {'type': 'string', 'index': 'not_analyzed'},
                        'tag': {
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
                                },
                            },
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
        for field in ('url', 'text', 'tag', 'xpath'):
            if field not in assoc or not assoc[field]:
                raise ValueError('missing field: %s' % field)
        for field in ('start_node', 'end_node'):
            if field not in assoc['xpath'] or not assoc['xpath'][field]:
                raise ValueError('missing xpath field: %s' % field)
        for field in ('start_idx', 'end_idx'):
            if field not in assoc['xpath'] or assoc['xpath'][field] is None:
                raise ValueError('missing xpath field: %s' % field)
        if len(assoc) > 4:
            raise ValueError('association object passed has too many fields')

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
        results = self.conn.search(index=self.index, doc_type=ty, body=query)
        return map(lambda r: r['_source'], results['hits']['hits'])
