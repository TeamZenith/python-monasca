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
import json
from oslo.config import cfg
from stevedore import driver

from monasca.common import es_conn
from monasca.common import kafka_conn
from monasca.openstack.common import log
from monasca.openstack.common import service as os_service

es_opts = [
    cfg.StrOpt('topic',
               default='alarmdefinitions',
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

es_group = cfg.OptGroup(name='alarmdefinitions', title='alarmdefinitions')
cfg.CONF.register_group(es_group)
cfg.CONF.register_opts(es_opts, es_group)

LOG = log.getLogger(__name__)


class AlarmDefintion(os_service.Service):
    def __init__(self, threads=1000):
        super(AlarmDefintion, self).__init__(threads)
        self._kafka_conn = kafka_conn.KafkaConnection(
            cfg.CONF.alarmdefinitions.topic)

        # Use doc_type if it is defined.
        if cfg.CONF.alarmdefinitions.doc_type:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.alarmdefinitions.doc_type)
        else:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.alarmdefinitions.topic)

    def start(self):
        while True:
            try:
                for msg in self._kafka_conn.get_messages():
                    if msg and msg.message:
                        LOG.debug("Message received for Alarm Definition methods: " + msg.message.value)
                        value = msg.message.value
                        if value:                            
                            alarmdefmessage = ast.literal_eval(value)
                            request_type = alarmdefmessage.pop("request", None)
                            id = alarmdefmessage["id"]

                            if request_type != None and id != None:
                                # post
                                if request_type == 'POST':
                                    self._es_conn.post_messages(json.dumps(alarmdefmessage), id)
                              
                # if autocommit is set, this will be a no-op call.
                self._kafka_conn.commit()
            except Exception:
                LOG.exception('Error occurred while handling alarmdefinitions kafka messages.')

    def stop(self):
        self._kafka_conn.close()
        super(Notification, self).stop()