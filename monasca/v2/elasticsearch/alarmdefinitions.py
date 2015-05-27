# Copyright 2015 Carnegie Mellon University
#
# Author: Shaunak Shatmanyu <shatmanyu@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import ast
import falcon
from oslo.config import cfg
import requests
import uuid

from monasca.common import es_conn
from monasca.common import kafka_conn
from monasca.common import resource_api
from monasca.openstack.common import log

try:
    import ujson as json
except ImportError:
    import json


alarmdefinitions_opts = [
    cfg.StrOpt('topic', default='alarmdefinitions',
               help='The topic that alarm definitions will be published to.'),
    cfg.IntOpt('size', default=1000,
               help=('The query result limit. Any result set more than '
                     'the limit will be discarded.')),
]

alarmdefinitions_group = cfg.OptGroup(name='alarmdefinitions', title='alarmdefinitions')
cfg.CONF.register_group(alarmdefinitions_group)
cfg.CONF.register_opts(alarmdefinitions_opts, alarmdefinitions_group)

LOG = log.getLogger(__name__)


class AlarmDefinitionUtil(object):

    @staticmethod
    def alarmdefinitionsparsing(req, q):
        try:
			name = req.get_param('name')
			if name and name.strip():
				q.append({'match': {'name': name.strip()}})
			
        except Exception:
            return False

        return True


class AlarmDefinitionDispatcher(object):
    def __init__(self, global_conf):
        LOG.debug('Initializing AlarmDefinition V2API!')
        super(AlarmDefinitionDispatcher, self).__init__()
        self.topic = cfg.CONF.alarmdefinitions.topic
        self.size = cfg.CONF.alarmdefinitions.size
        self._kafka_conn = kafka_conn.KafkaConnection(self.topic)
        self._es_conn = es_conn.ESConnection(self.topic)

        # Setup the get alarm definitions query url pattern
        self._query_url = ''.join([self._es_conn.uri,
                                  self._es_conn.index_prefix, '*/',
                                  cfg.CONF.alarmdefinitions.topic,
                                  '/_search?search_type=alarmdef'])

	def post_data(self, req, res):
		LOG.debug('Creating the alarm definitions')
        msg = req.stream.read()

		post_msg = ast.literal_eval(msg)
		
		# random uuid genearation for alarm definition
		id = str(uuid.uuid4())
		
		post_msg["id"] = id
		post_msg["request"] = "POST"
		
        code = self._kafka_conn.send_messages(json.dumps(msg))
        res.status = getattr(falcon, 'HTTP_' + str(code))
	
    def _get_alarm_definitions_response(self, res):
        if res and res.status_code == 200:
            obj = res.json()
            if obj:
                return obj.get('alarmdefinitions')
            return None
        else:
            return None
 

    @resource_api.Restify('/v2.0/alarm-definitions/', method='post')
    def do_post_alarm_definitions(self, req, res):
        self.post_data(req, res)

    @resource_api.Restify('/v2.0/alarm-definitions/', method='get')
    def do_get_alarm_definitions(self, req, res):
        LOG.debug('The alarm definitions GET request is received!')
        # process query conditions
        query = []
        alarmdefinitionsparsing(req, query)
        body = '' + query

        LOG.debug('Request body:' + body)
        es_res = requests.post(self._query_url, data=body)
        res.status = getattr(falcon, 'HTTP_%s' % es_res.status_code)

        LOG.debug('Query to ElasticSearch returned: %s' % es_res.status_code)
        res_data = self._get_alarm_definitions_response(es_res)
        if res_data:
            res.body = ''.join(res_data)
            res.content_type = 'application/json;charset=utf-8'
        else:
            res.body = ''