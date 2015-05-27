# Copyright 2015 Carnegie Mellon University
#
# Author: Han Chen <hanc@andrew.cmu.edu>
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
import json
from oslo.config import cfg
from stevedore import driver

from monasca.common import es_conn
from monasca.common import email_sender
from monasca.common import kafka_conn
from monasca.openstack.common import log
from monasca.openstack.common import service as os_service

es_opts = [
    cfg.StrOpt('topic',
               default='alarm',
               help=('The topic that messages will be retrieved from.'
                     'This also will be used as a doc type when saved '
                     'to ElasticSearch.')),

    cfg.StrOpt('topic2',
               default='notification_methods',
               help=('The topic that messages will be retrieved from.'
                     'This also will be used as a doc type when saved '
                     'to ElasticSearch.')),

    cfg.StrOpt('doc_type',
               default='',
               help=('The document type which defines what document '
                     'type the messages will be save into. If not '
                     'specified, then the topic will be used.')),
    cfg.StrOpt('processor',
               default='',
               help=('The message processer to load to process the message.'
                     'If the message does not need to be process anyway,'
                     'leave the default')),
]

es_group = cfg.OptGroup(name='notification', title='notification')
cfg.CONF.register_group(es_group)
cfg.CONF.register_opts(es_opts, es_group)

LOG = log.getLogger(__name__)


class NotificationEngine(os_service.Service):
    def __init__(self, threads=1000):
        super(NotificationEngine, self).__init__(threads)
        self._kafka_conn = kafka_conn.KafkaConnection(
            cfg.CONF.notification.topic)

        # Use doc_type if it is defined.
        if cfg.CONF.notification.doc_type:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.notification.doc_type)
        else:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.notification.topic2)

    def handle_alarm_msg(self, msg):
        if msg and msg.message:
            LOG.debug("Message received for alarm: " + msg.message.value)
            value = msg.message.value
            if value:
                # value's format is:
                # {
                #   "metrics": {
                #     "timestamp": 1432672915.409,
                #     "name": "biz",
                #     "value": 1500,
                #     "dimensions": {
                #       "key2": "value2",
                #       "key1": "value1"
                #     }
                #   },
                #   "state_updated_timestamp": 1432672915,
                #   "state": "ALARM",
                #   "alarm-definition": {
                #     "alarm_actions": [
                #       "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
                #     ],
                #     "undetermined_actions": [
                #       "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
                #     ],
                #     "name": "Average CPU percent greater than 10",
                #     "match_by": [
                #       "hostname"
                #     ],
                #     "description": "The average CPU percent is greater than 10",
                #     "ok_actions": [
                #       "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
                #     ],
                #     "expression": "max(foo{hostname=mini-mon,mu=na}, 120) > 1100
                #                   and max(bar { asd = asd} )>1200 or avg(biz)>1300",
                #     "id": "c60ec47e-5038-4bf1-9f95-4046c6e91111",
                #     "severity": "LOW"
                #   }
                # }

                # convert to dict, and get state to determine the actions(notification method id) needed.
                # the method id can be used to match the notification method in elasticSearch
                # Then an email will be sent (TODO: phone txt msg are not dealt with for now)
                dict_msg = ast.literal_eval(value)
                state = dict_msg["state"]
                if state not in ["ALARM","OK","UNDETERMINED"]:
                    LOG.error("state of alarm is not defined as expected")
                    return

                actions = []
                if state == 'ALARM':
                    actions = dict_msg["alarm-definition"]["alarm_actions"]
                if state == 'OK':
                    actions = dict_msg["alarm-definition"]["ok_actions"]
                if state == 'UNDETERMINED':
                    actions = dict_msg["alarm-definition"]["undetermined_actions"]

                addresses = []
                types = []
                # the action_id is an id of notification method
                # there can be multiple ids in one alarm message with different types
                for action_id in actions:

                    es_res = self._es_conn.get_message_by_id(action_id)

                    def _get_notification_method_response(res):
                        if res and res.status_code == 200:
                            obj = res.json()
                            if obj:
                                return obj.get('hits')
                            return None
                        else:
                            return None

                    es_res = _get_notification_method_response(es_res)

                    LOG.debug('Query to ElasticSearch returned: %s' % es_res)

                    if es_res is None:
                        LOG.error("The provided is not defined as expected")
                        return

                    name = es_res["hits"][0]["_source"]["name"]
                    type = es_res["hits"][0]["_source"]["type"]
                    address = es_res["hits"][0]["_source"]["address"]

                    types.append(type)
                    addresses.append(address)

                email_addresses = []
                for i in range(len(types)):
                    if types[i] == "EMAIL":
                        email_addresses.append(addresses[i])

                email_sender.send_emails(email_addresses, "Alarm to User", dict_msg["alarm-definition"]["description"])

    def start(self):
        while True:
            try:
                for msg in self._kafka_conn.get_messages():
                    self.handle_alarm_msg(msg)

                # if autocommit is set, this will be a no-op call.
                self._kafka_conn.commit()
            except Exception:
                LOG.exception('Error occurred while handling kafka messages.')

    def stop(self):
        self._kafka_conn.close()
        super(NotificationEngine, self).stop()
