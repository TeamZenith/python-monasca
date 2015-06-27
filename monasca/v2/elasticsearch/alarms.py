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
import uuid

from monasca.common import es_conn
from monasca.common import kafka_conn
from monasca.common import resource_api
from monasca.openstack.common import log


try:
    import ujson as json
except ImportError:
    import json


alarms_opts = [
    cfg.StrOpt('topic', default='alarms',
               help='The topic that alarms will be published to.'),
    cfg.IntOpt('size', default=1000,
               help=('The query result limit. Any result set more than '
                     'the limit will be discarded.')),
]


alarms_group = cfg.OptGroup(name='alarms', title='alarms')
cfg.CONF.register_group(alarms_group)
cfg.CONF.register_opts(alarms_opts, alarms_group)

LOG = log.getLogger(__name__)


class AlarmUtil(object):

    @staticmethod
    def severityparsing(msg):
        try:
            severity = msg["severity"]
            if severity == 'LOW' or severity == 'MEDIUM' or severity == 'HIGH' or severity == 'CRITICAL':
                return msg
            else:
                msg["severity"] = "LOW"
                return msg
        except Exception:
            return msg


class AlarmDispatcher(object):

    def __init__(self, global_conf):
        LOG.debug('Initializing Alarms V2API!')
        super(AlarmDispatcher, self).__init__()
        self.topic = cfg.CONF.alarms.topic
        self.size = cfg.CONF.alarms.size
        self._kafka_conn = kafka_conn.KafkaConnection(self.topic)
        self._es_conn = es_conn.ESConnection(self.topic)

    def post_data(self, req, res):
        LOG.debug('Creating the alarms')
        msg = req.stream.read()
        post_msg = ast.literal_eval(msg)

        # random uuid genearation for alarm definition
        id = str(uuid.uuid4())
        post_msg["id"] = id
        post_msg["request"] = "POST"

        LOG.debug("Post Alarm method: %s" % post_msg)
        code = self._kafka_conn.send_messages(json.dumps(post_msg))
        res.status = getattr(falcon, 'HTTP_' + str(code))

    def _get_alarms_response(self, res):
        if res and res.status_code == 200:
            obj = res.json()
            if obj:
                return obj.get('hits')
            return None
        else:
            return None

    # def get_alarms_helper():

    @resource_api.Restify('/v2.0/alarms/', method='post')
    def do_post_alarms(self, req, res):
        self.post_data(req, res)

    @resource_api.Restify('/v2.0/alarms/{id}', method='get')
    def do_get_alarms_by_id(self, req, res, id):
        LOG.debug('The alarms by id GET request is received!')
        LOG.debug(id)

        es_res = self._es_conn.get_message_by_id(id)
        res.status = getattr(falcon, 'HTTP_%s' % es_res.status_code)

        LOG.debug('Query to ElasticSearch returned Status: %s' %
                  es_res.status_code)
        es_res = self._get_alarms_response(es_res)
        LOG.debug('Query to ElasticSearch returned: %s' % es_res)

        res_data = es_res["hits"][0]
        if res_data:
            res.body = json.dumps([{
                "id": id,
                "links": [{"rel": "self",
                           "href": req.uri}],
                "metrics":res_data["_source"]["metrics"],
                "state":res_data["_source"]["state"],
                "sub_alarms":res_data["_source"]["sub_alarms"],
                "state_updated_timestamp":res_data["_source"]["state_updated_timestamp"],
                "updated_timestamp":res_data["_source"]["updated_timestamp"],
                "created_timestamp":res_data["_source"]["created_timestamp"]}])

            res.content_type = 'application/json;charset=utf-8'
        else:
            res.body = ''

    @resource_api.Restify('/v2.0/alarms/', method='get')
    def do_get_alarms(self, req, res):
        LOG.debug('The alarms GET request is received!')
        LOG.debug('Request: %s' % req)
        LOG.debug(falcon.request.helpers.Body)
        #LOG.debug(req.get('BODY'))
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')

        try:
            LOG.debug(json.loads(body.decode('utf-8')))
            req.context['doc'] = json.loads(body.decode('utf-8'))

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect or not encoded as '
                                   'UTF-8.')

        query = ''
        if req["alarm_defintiion_id"]:
            query += "alarm_definition_id:" + req["alarm_defintiion_id"]
        if req["metric_name"]:
            query += "metric_name:" + req["metric_name"]

        es_res = self._es_conn.get_alarms(query)
        res.status = getattr(falcon, 'HTTP_%s' % es_res.status_code)

        LOG.debug('Query to ElasticSearch returned Status: %s' %
                  es_res.status_code)
        es_res = self._get_alarms_response(es_res)
        LOG.debug('Query to ElasticSearch returned: %s' % es_res)

        res_data = es_res["hits"][0]
        if res_data:
            res.body = json.dumps([{
                "id": id,
                "links": [{"rel": "self",
                           "href": req.uri}],
                "alarm_definition_id": res_data["_source"]["alarm_definition_id"],
                "metrics":res_data["_source"]["metrics"],
                "state":res_data["_source"]["state"],
                "lifecycle_state":res_data["_source"]["lifecycle_state"],
                "link":res_data["_source"]["link"],
                "state_updated_timestamp":res_data["_source"]["state_updated_timestamp"],
                "updated_timestamp":res_data["_source"]["updated_timestamp"],
                "created_timestamp":res_data["_source"]["created_timestamp"]}])

            res.content_type = 'application/json;charset=utf-8'
        else:
            res.body = ''
