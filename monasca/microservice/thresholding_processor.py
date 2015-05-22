# Copyright 2013 IBM Corp
#
# Author: Tong Li <litong01@us.ibm.com>
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

import json
import time
import urllib
from monasca.openstack.common import log
from monasca.common import alarm_expr_parser_nilwyh as parser
LOG = log.getLogger(__name__)


def calc_value(data_list, func):
    value = 0.0
    if func == 'sum':
        value = 0.0
        for x in data_list:
            value += x[1]
    elif func == 'avg':
        value = 0.0
        for x in data_list:
            value += x[1]
        value /= len(data_list)
    elif func == 'max':
        value = data_list[0][1]
        for x in data_list:
            value = max(value, x[1])
    elif func == 'min':
        value = data_list[0][1]
        for x in data_list:
            value = min(value, x[1])
    elif func == 'count':
        value = len(data_list)
    return value


def get_state(logic_tree):
    return get_sub_state(logic_tree)


def get_sub_state(root):
    if not root.has_key('value'):
        return root['state']
    if root['value'] == 'and':
        ls = get_sub_state(root['left'])
        rs = get_sub_state(root['right'])
        if ls == 'UNDETERMINED' or rs == 'UNDETERMINED':
            return 'UNDETERMINED'
        if ls == 'OK' or rs =='OK':
            return 'OK'
        else:
            return 'ALARM'
    elif root['value'] == 'or':
        ls = get_sub_state(root['left'])
        rs = get_sub_state(root['right'])
        if ls == 'ALARM' or rs == 'ALARM':
            return 'ALARM'
        if ls == 'UNDETERMINED' or rs =='UNDETERMINED':
            return 'UNDETERMINED'
        else:
            return 'OK'
    return 'UNDETERMINED'


class ThresholdingProcessor(object):
    def __init__(self, alarm_def):
        LOG.debug('initializing AlarmProcessor!')
        super(ThresholdingProcessor, self).__init__()
        self.alarm_definition = json.loads(alarm_def)
        #(avg(cpu,user_perc{hostname=devstack}) > 10)
        self.expression = self.alarm_definition['expression']
        alarm_parser = parser.AlarmExprParser(self.expression)
        self.logic_tree = alarm_parser.get_logic_tree()
        self.sub_expression = alarm_parser.get_sub_expr_list()
        self.state_current = 'UNDETERMINED'

    def process_msg(self, msg):
        try:
            data = json.loads(msg)
            updated = False
            for sub_expr in self.sub_expression:
                if self.update_expr(sub_expr, data):
                    updated = True
            if not updated:
                return None
            state_new = get_state(self.logic_tree)
            if state_new != self.state_current:
                self.state_current = state_new
                return self.build_alarm(data)
            else:
                return None
        except Exception:
            #LOG.exception('')
            return None

    def update_expr(self, sub_expr, data):
        name = data['name']
        dimensions = data['dimensions']

        if name != sub_expr['metrics_name']:
            return False
        for dimension in sub_expr['dimensions']:
            if dimensions[dimension]:
                if dimensions[dimension] != sub_expr['dimensions'][dimension]:
                    return False
            else:
                return False
        # a deque
        data_list = sub_expr['data_list']
        data_list.append((data['timestamp'], data['value']))
        # assume later timestamp metrics will be received by engine later
        start_time = int(data['timestamp']) - int(sub_expr['period'])
        while len(data_list) != 0 and data_list[0][0] < start_time:
            data_list.popleft()
        if len(data_list) == 0:
            sub_expr['state'] = 'UNDETERMINED'
            return True

        value_cur = calc_value(data_list, sub_expr['function'])
        value_thresh = sub_expr['threshold']
        op = sub_expr['operator']
        state = 'UNDETERMINED'
        if op == 'GT' and value_cur > value_thresh:
            state = 'ALARM'
        elif op == 'LT' and value_thresh > value_cur:
            state = 'ALARM'
        elif op == 'LTE' and value_cur <= value_thresh:
            state = 'ALARM'
        elif op == 'GTE' and value_thresh <= value_cur:
            state = 'ALARM'
        else:
            state = 'OK'
        sub_expr['state'] = state
        return True

    def build_alarm(self, data):
        alarm = {}
        alarm['alarm-definition'] = self.alarm_definition

        alarm['metrics'] = data
        alarm['state'] = self.state_current

        now = int(time.time())
        alarm['state_updated_timestamp'] = now

        if self.alarm_definition.has_key('timestamp'):
            alarm['created_timestamp'] = self.alarm_definition['timestamp']
        """
        alarm_id = (urllib.quote(self.alarm_definition['id'].encode('utf8'), safe='')
                    + '?' + urllib.quote(now, safe=''))
        alarm['id'] = alarm_id

        """
        return json.dumps(alarm)
