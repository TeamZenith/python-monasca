#
# Copyright 2012-2013 eNovance <licensing@enovance.com>
#
# Author: Julien Danjou <julien@danjou.info>
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
from oslo.config import types
from oslo.config import cfg
from stevedore import driver
import json
from monasca.common import es_conn
from monasca.common import kafka_conn
from monasca.openstack.common import log
from monasca.openstack.common import service as os_service


PROCESSOR_NAMESPACE = 'monasca.message.processor'

th_opts = [
    cfg.MultiOpt('consume_topic', item_type=types.String(),
                 default=['event','metrics','alarmdefinitions'],
                 help='input topics'),
    cfg.MultiOpt('publish_topic', item_type=types.String(),
                 default=['event', 'alarmdefinitions', 'alarm'],
                 help='output topics')
]

th_group = cfg.OptGroup(name='thresholding_engine', title='thresholding_engine')
cfg.CONF.register_group(th_group)
cfg.CONF.register_opts(th_opts, th_group)

LOG = log.getLogger(__name__)


class ThresholdingEngine(os_service.Service):

    def __init__(self, threads=1000):
        super(ThresholdingEngine, self).__init__(threads)
        self._consume_kafka_conn = {}
        self._publish_kafka_conn = {}
        for topic in self.cfg.CONF.thresholding_engine.consume_topic:
            self._consume_kafka_conn[topic] = kafka_conn.KafkaConnection(topic)

        for topic in self.cfg.CONF.thresholding_engine.publish_topic:
            self._publish_kafka_conn[topic] = kafka_conn.KafkaConnection(topic)

        self.thresholding_processors={}

    def start(self):
        while True:
            try:

                if self._consume_kafka_conn.has_key('alarmdefinitions'):
                    for msg in self._consume_kafka_conn['alarmdefinitions'].get_messages():
                        if msg and msg.message:
                            LOG.debug(msg.message.value)
                            temp_admin = json.loads(msg.message.value)
                            self.thresholding_processors[temp_admin['name']] = driver.DriverManager(
                                PROCESSOR_NAMESPACE,
                                cfg.CONF.thresholding_engine.processor,
                                invoke_on_load=True,
                                invoke_kwds=(msg.message.value)).driver
                            LOG.debug(dir(self.thresholding_processors[temp_admin['name']]))
                        self._consume_kafka_conn['alarmdefinitions'].commit()

                    if self._consume_kafka_conn.has_key('metrics'):
                        for msg in self._consume_kafka_conn['metrics'].get_messages():
                            if msg and msg.message:
                                LOG.debug(msg.message.value)
                                for alarm_def in self.thresholding_processors.keys():
                                    alarm = self.thresholding_processors[alarm_def].process_msg(
                                        msg.message.value)
                                    if alarm and self._publish_kafka_conn['alarm']:
                                        self._publish_kafka_conn['alarm'].send_messages(alarm)

                        self._publish_kafka_conn['alarm'].commit()

            except Exception:
                LOG.exception('Error occurred while handling kafka messages.')

    def stop(self):
        for topic in self._consume_kafka_conn.keys():
            self._consume_kafka_conn[topic].close()
        for topic in self._publish_kafka_conn.keys():
            self._publish_kafka_conn[topic].close()
        super(ThresholdingEngine, self).stop()