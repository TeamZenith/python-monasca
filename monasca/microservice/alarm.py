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

from monasca.common import es_conn
from monasca.common import kafka_conn
from monasca.openstack.common import log
from monasca.openstack.common import service as os_service

es_opts = [
    cfg.StrOpt('topic',
               default='alarms',
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

es_group = cfg.OptGroup(name='alarms', title='alarms')
cfg.CONF.register_group(es_group)
cfg.CONF.register_opts(es_opts, es_group)

LOG = log.getLogger(__name__)


class Alarm(os_service.Service):

    def __init__(self, threads=1000):
        super(Alarm, self).__init__(threads)
        self._kafka_conn = kafka_conn.KafkaConnection(
            cfg.CONF.alarms.topic)

        # Use doc_type if it is defined.
        if cfg.CONF.alarms.doc_type:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.alarms.doc_type)
        else:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.alarms.topic)

    def start(self):
        while True:
            try:
                for msg in self._kafka_conn.get_messages():
                    if msg and msg.message:
                        LOG.debug(
                            "Message received for Alarm methods: " + msg.message.value)
                        value = msg.message.value

                        if value:
                            alarmmessage = ast.literal_eval(value)
                            request_type = alarmmessage.pop("request", None)
                            id = alarmmessage["id"]

                            if request_type is not None and id is not None:
                                # post
                                if request_type == 'POST':
                                    self._es_conn.post_messages(
                                        json.dumps(alarmmessage), id)

                                # put
                                if request_type == 'PUT':
                                    self._es_conn.put_messages(
                                        json.dumps(alarmmessage), id)

                                # delete
                                if request_type == 'DEL':
                                    self._es_conn.del_messages(id)

                # if autocommit is set, this will be a no-op call.
                self._kafka_conn.commit()
            except Exception:
                LOG.exception(
                    'Error occurred while handling kafka messages for Alarms.')

    def stop(self):
        self._kafka_conn.close()
        super(Alarm, self).stop()
