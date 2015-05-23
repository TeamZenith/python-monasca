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

import falcon
from oslo.config import cfg
import requests

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
            
			description = req.get_param('description')
			if description:
				q.append({'match': {'description': description}})
			
			expression = req.get_param('expression')
			if expression:
				q.append({'match': {'expression': expression}})
			
			expression_data = req.get_param('expression_data')
			if expression_data:
				q.append({'match': {'expression_data': expression_data}})
			
			match_by = req.get_param('match_by')
			if match_by:
				q.append({'match': {'match_by': match_by}})
				
			severity = req.get_param('severity')
			if severity:
				q.append({'match': {'severity': severity}})
				
			actions_enabled = req.get_param('actions_enabled')
			if actions_enabled:
				q.append({'match': {'actions_enabled': actions_enabled}})
			
			alarm_actions = req.get_param('alarm_actions')
			if alarm_actions:
				q.append({'match': {'alarm_actions': alarm_actions}})
			
			ok_actions = req.get_param('ok_actions')
			if ok_actions:
				q.append({'match': {'ok_actions': ok_actions}})
				
			undetermined_actions = req.get_param('undetermined_actions')
			if undetermined_actions:
				q.append({'match': {'undetermined_actions': undetermined_actions}})
			
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

        # Setup the get alarm definitions query body pattern
        self._query_body = {
            "query": {"bool": {"must": []}},
            "size": self.size}

        self._query_url = ''.join([self._es_conn.uri,
                                  self._es_conn.index_prefix, '*/',
                                  cfg.CONF.alarmdefinitions.topic,
                                  '/_search?search_type=alarmdef'])

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
        LOG.debug('Creating the alarm definitions')
        msg = req.stream.read()

        code = self._kafka_conn.send_messages(msg)
        res.status = getattr(falcon, 'HTTP_' + str(code))

    @resource_api.Restify('/v2.0/alarm-definitions/', method='get')
    def do_get_alarm_definitions(self, req, res):
        LOG.debug('The alarm definitions GET request is received!')
        # process query conditions
        query = []
        alarmdefinitionsparsing(req, query)
        _measure_ag = self._measure_agg % {"size": self.size}
        if query:
            body = ('{"query":{"bool":{"must":' + json.dumps(query) + '}},'
                    '"size":' + str(self.size) + ','
        else:
            body = ''

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