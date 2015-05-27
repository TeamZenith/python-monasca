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
from monasca.common import kafka_conn
from monasca.openstack.common import log
from monasca.openstack.common import service as os_service

es_opts = [
    cfg.StrOpt('topic',
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


class Notification(os_service.Service):
    def __init__(self, threads=1000):
        super(Notification, self).__init__(threads)
        self._kafka_conn = kafka_conn.KafkaConnection(
            cfg.CONF.notification.topic)

        # Use doc_type if it is defined.
        if cfg.CONF.notification.doc_type:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.notification.doc_type)
        else:
            self._es_conn = es_conn.ESConnection(
                cfg.CONF.notification.topic)

    def handle_notification_msg(self, msg):
        if msg and msg.message:
            LOG.debug("Message received for Notification methods: " + msg.message.value)
            value = msg.message.value
            if value:
                # value's format is:
                # {"id":"c60ec47e-5038-4bf1-9f95-4046c6e9a759",
                # "request":"POST",
                # "name":"TheName",
                # "type":"TheType",
                # "Address":"TheAddress"}
                # We add the POS/PUT/DEL in the message to indicate the request type

                # Get the notification id from the message,
                # this id will be used as _id for elasticsearch,
                # and also stored as id in the notification_methods document type

                # convert to dict, pop request, and get id
                # after request is removed, the dict can be converted to request body for elasticsearch
                dict_msg = ast.literal_eval(value)
                request_type = dict_msg.pop("request", None)
                id = dict_msg["id"]

                if request_type != None and id != None:
                    # post
                    if request_type == 'POST':
                        self._es_conn.post_messages(json.dumps(dict_msg), id)

                    # put
                    if request_type == 'PUT':
                        self._es_conn.put_messages(json.dumps(dict_msg), id)

                    # delete
                    if request_type == 'DEL':
                        self._es_conn.del_messages(id)

    def start(self):
        while True:
            try:
                for msg in self._kafka_conn.get_messages():
                    self.handle_notification_msg(msg)

                # if autocommit is set, this will be a no-op call.
                self._kafka_conn.commit()
            except Exception:
                LOG.exception('Error occurred while handling kafka messages.')

    def stop(self):
        self._kafka_conn.close()
        super(Notification, self).stop()
