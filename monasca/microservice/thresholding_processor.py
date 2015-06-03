# Copyright 2015 CMU
# Author: Yihan Wang <wangff9@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import json
from monasca.common import alarm_expr_calculator as calculator
from monasca.common import alarm_expr_parser as parser
from monasca.openstack.common import log
import time
import uuid


LOG = log.getLogger(__name__)


class ThresholdingProcessor(object):
    def __init__(self, alarm_def):
        """One processor instance hold one alarm definition."""
        LOG.debug('initializing ThresholdProcessor!')
        super(ThresholdingProcessor, self).__init__()
        self.alarm_definition = json.loads(alarm_def)
        self.expression = self.alarm_definition['expression']
        self.match_by = self.alarm_definition['match_by']
        self.expr_data_queue = {}
        if len(self.match_by) == 0:
            self.match_by = None
        self.parse_result = (
            parser.AlarmExprParser(self.expression).parse_result)
        self.sub_expr_list = self.parse_result.operands_list
        LOG.debug('successfully initialize ThresholdProcessor!')

    def process_metrics(self, metrics):
        """Add new metrics to matched expr."""
        try:
            data = json.loads(metrics)
            self.add_expr_metrics(data)
        except Exception:
            LOG.exception('process metrics error')

    def process_alarms(self):
        """Run every minute to produce alarm."""
        try:
            alarm_list = []
            for m in self.expr_data_queue.keys():
                if_updated = self.update_state(self.expr_data_queue[m])
                if if_updated:
                    alarm_list.append(self.build_alarm(m))
            return alarm_list
        except Exception:
            LOG.exception('process metrics error')
            return []

    def update_state(self, expr_data):
        """Update the state of each alarm under this alarm definition."""

        def _calc_state(operand):
            if operand.logic_operator:
                subs = []
                for o in operand.sub_expr_list:
                    subs.append(_calc_state(o))
                return calculator.calc_logic(operand.logic_operator, subs)
            else:
                return expr_data['data'][operand.fmtd_sub_expr_str]['state']

        for sub_expr in self.sub_expr_list:
            self.update_sub_expr_state(sub_expr, expr_data)
        state_new = _calc_state(self.parse_result)
        if state_new != expr_data['state']:
            expr_data['state'] = state_new
            return True
        else:
            return False

    def update_sub_expr_state(self, expr, expr_data):
        def _update_metrics():
            """Delete metrics not in period."""
            data_list = expr_data['data'][expr.fmtd_sub_expr_str]['metrics']
            start_time = t_now - (float(expr.period) + 2) * int(expr.periods)
            while (len(data_list) != 0
                   and data_list[0]['timestamp'] < start_time):
                data_list.popleft()

        def _update_state():
            """Update state of a sub expr."""
            data_sub = expr_data['data'][expr.fmtd_sub_expr_str]
            data_list = data_sub['metrics']
            if len(data_list) == 0:
                data_sub['state'] = 'UNDETERMINED'
            else:
                period = float(expr.period)
                right = t_now
                left = right - period
                temp_data = []
                value_in_periods = []
                for i in range(len(data_list) - 1, -1, -1):
                    if data_list[i]['timestamp'] >= left:
                        temp_data.append(data_list[i]['value'])
                    else:
                        value = calculator.calc_value(
                            expr.normalized_func, temp_data)
                        value_in_periods.append(value)
                        right = left
                        left = right - period
                        temp_data = []
                value = calculator.calc_value(
                    expr.normalized_func, temp_data)
                value_in_periods.append(value)
                expr_data['data'][expr.fmtd_sub_expr_str]['state'] = (
                    calculator.compare_thresh(
                        value_in_periods,
                        expr.normalized_operator,
                        float(expr.threshold)))

        t_now = time.time()
        _update_metrics()
        _update_state()

    def add_expr_metrics(self, data):
        """Add new metrics to matched place."""
        for sub_expr in self.sub_expr_list:
            self.add_sub_expr_metrics(sub_expr, data)

    def add_sub_expr_metrics(self, expr, data):
        """Add new metrics to sub expr place."""

        def _has_match_expr():
            if (data['name'].lower().encode('utf8') !=
                    expr.normalized_metric_name.encode('utf8')):
                return False
            metrics_dimensions = {}
            if 'dimensions' in data:
                metrics_dimensions = data['dimensions']
            def_dimensions = expr.dimensions_as_dict
            for dimension_key in def_dimensions.keys():
                if dimension_key in metrics_dimensions:
                    if (metrics_dimensions[
                            dimension_key].lower().encode('utf8')
                            != def_dimensions[
                            dimension_key].lower().encode('utf8')):
                        return False
                else:
                    return False
            return True

        def _add_metrics():
            if self.match_by:
                q_name = self.get_matched_data_queue_name(data)
                if q_name:
                    temp = self.expr_data_queue[q_name]['data']
                    data_list = temp[expr.fmtd_sub_expr_str]['metrics']
                    data_list.append(data)
            else:
                if None not in self.expr_data_queue:
                    self.create_data_item(None)
                temp = self.expr_data_queue[None]['data']
                data_list = temp[expr.fmtd_sub_expr_str]['metrics']
                data_list.append(data)

        if _has_match_expr():
            _add_metrics()

    def create_data_item(self, name):
        """If not exist in dict, create one item."""
        ts = time.time()
        self.expr_data_queue[name] = {
            'data': {},
            'state': 'UNDETERMINED',
            'create_timestamp': ts,
            'update_timestamp': ts}
        for expr in self.sub_expr_list:
            self.expr_data_queue[name]['data'][expr.fmtd_sub_expr_str] = {
                'state': 'UNDETERMINED',
                'metrics': collections.deque()}

    def get_matched_data_queue_name(self, data):
        name = ''
        for m in self.match_by:
            if m in data['dimensions']:
                name = name + data['dimensions'][m]
            else:
                return None
        if name in self.expr_data_queue:
            return name
        else:
            self.create_data_item(name)
            return name

    def build_alarm(self, name):
        """Build alarm json."""
        alarm = {}
        id = str(uuid.uuid4())
        alarm['id'] = id
        alarm['alarm-definition'] = self.alarm_definition
        alarm['metrics'] = self.get_all_metrics(name)
        alarm['state'] = self.expr_data_queue[name]['state']
        t = self.expr_data_queue[name]['update_timestamp']
        alarm['state_updated_timestamp'] = t
        alarm['updated_timestamp'] = t
        alarm['created_timestamp'] = (
            self.expr_data_queue[name]['create_timestamp'])
        return json.dumps(alarm)

    def get_all_metrics(self, name):
        """Get all metrics related to one alarm."""
        metrics_list = []
        for expr in self.expr_data_queue[name]['data'].keys():
            for metrics in self.expr_data_queue[name]['data'][expr]['metrics']:
                metrics_list.append(metrics)
        return metrics_list
