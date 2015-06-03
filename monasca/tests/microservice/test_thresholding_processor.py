# -*- coding: utf-8 -*-
# Copyright 2015 CMU
# Author: Yihan Wang <wangff9@gmail.com>
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
from monasca.microservice import thresholding_processor as processor
from monasca.openstack.common import log
from monasca import tests
import time

LOG = log.getLogger(__name__)


class TestThresholdingProcessor(tests.BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestThresholdingProcessor, self).__init__(*args, **kwargs)
        self.alarm_definition0 = json.dumps({
            "id": "f9935bcc-9641-4cbf-8224-0993a947ea83",
            "name": "Average CPU percent greater than 10",
            "description":
                "The average CPU percent is greater than 10",
            "expression":
                "max(-_.千幸福的笑脸{घोड़ा=馬,  "
                "dn2=dv2,"
                "千幸福的笑脸घ=千幸福的笑脸घ}) gte 100 "
                "times 1 And "
                "(min(ເຮືອນ{dn3=dv3,家=дом}) < 10 "
                "or sum(biz{dn5=dv58}) >9 9and "
                "count(fizzle) lt 0 or count(baz) > 1)",
            "match_by": [],
            "severity": "LOW",
            "ok_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "alarm_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "undetermined_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ]})
        self.alarm_definition1 = json.dumps({
            "id": "f9935bcc-9641-4cbf-8224-0993a947ea83",
            "name": "Average CPU percent greater than 10",
            "description":
                "The average CPU percent is greater than 10",
            "expression": "max(biz)>100",
            "match_by": [
                "hostname"
            ],
            "severity": "LOW",
            "ok_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "alarm_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "undetermined_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ]})
        self.alarm_definition2 = json.dumps({
            "id": "f9935bcc-9641-4cbf-8224-0993a947ea83",
            "name": "Average CPU percent greater than 10",
            "description":
                "The average CPU percent is greater than 10",
            "expression": "max(foo)>=100 times 4",
            "match_by": [],
            "severity": "LOW",
            "ok_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "alarm_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "undetermined_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ]})
        self.alarm_definition3 = json.dumps({
            "id": "f9935bcc-9641-4cbf-8224-0993a947ea83",
            "name": "Average CPU percent greater than 10",
            "description":
                "The average CPU percent is greater than 10",
            "expression": "max(foo{hostname=mini-mon"
                          ",千=千}, 120)"
                          " = 100 and (max(bar)>100 "
                          " or max(biz)>100)",
            "match_by": [],
            "severity": "LOW",
            "ok_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "alarm_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ],
            "undetermined_actions": [
                "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
            ]})

    def getMetric2_0(self):
        list = []
        for i in range(0, 10, 1):
            metrics = {"name": "foo",
                       "dimensions": {
                           "key1": "value1",
                           "key2": "value2"
                       },
                       "timestamp": time.time() + i * 20 - 200,
                       "value": i * 10}
            list.append(json.dumps(metrics))
        return list

    def getMetric2_1(self):
        list = []
        for i in range(10, 30, 1):
            metrics = {"name": "foo",
                       "dimensions": {
                           "key1": "value1",
                           "key2": "value2"
                       },
                       "timestamp": time.time() + i * 20 - 570,
                       "value": i * 75}
            list.append(json.dumps(metrics))
        return list

    def getMetric1(self):
        list = []
        metrics = {"name": "biz",
                   "dimensions": {
                       "hostname": "h1",
                       "key2": "value2"
                   },
                   "timestamp": time.time(),
                   "value": 1500}
        list.append(json.dumps(metrics))
        metrics = {"name": "biz",
                   "dimensions": {
                       "hostname": "h2",
                       "key2": "value2"
                   },
                   "timestamp": time.time(),
                   "value": 1500}
        list.append(json.dumps(metrics))
        metrics = {"name": "biz",
                   "dimensions": {
                       "hostname": "h3",
                       "key2": "value2"
                   },
                   "timestamp": time.time(),
                   "value": 1500}
        list.append(json.dumps(metrics))
        return list

    def getMetric0(self):
        list = []
        metrics = {"name": "baz",
                   "dimensions": {
                       "घोड़ा": "馬",
                       "dn2": "dv2",
                       "千幸福的笑脸घ": "千幸福的笑脸घ"
                   },
                   "timestamp": time.time(),
                   "value": 1500}
        list.append(json.dumps(metrics))
        metrics = {"name": "-_.千幸福的笑脸",
                   "dimensions": {
                       "घोड़ा": "馬",
                       "dn2": "dv2",
                       "千幸福的笑脸घ": "千幸福的笑脸घ"
                   },
                   "timestamp": time.time(),
                   "value": 1500}
        list.append(json.dumps(metrics))
        metrics = {"name": "ເຮືອນ",
                   "dimensions": {
                       "dn3": "dv3",
                       "家": "дом"
                   },
                   "timestamp": time.time(),
                   "value": 5}
        list.append(json.dumps(metrics))
        metrics = {"name": "biz",
                   "dimensions": {
                       "dn5": "dv58"
                   },
                   "timestamp": time.time(),
                   "value": 5}
        list.append(json.dumps(metrics))
        metrics = {"name": "biz",
                   "dimensions": {
                       "dn5": "dv58"
                   },
                   "timestamp": time.time(),
                   "value": 95}
        list.append(json.dumps(metrics))
        return list

    def setUp(self):
        super(TestThresholdingProcessor, self).setUp()

    def test__init_(self):
        tp = None
        try:
            tp = processor.ThresholdingProcessor(self.alarm_definition0)
        except Exception:
            tp = None
        self.assertIsInstance(tp, processor.ThresholdingProcessor)
        try:
            tp = processor.ThresholdingProcessor(self.alarm_definition1)
        except Exception:
            tp = None
        self.assertIsInstance(tp, processor.ThresholdingProcessor)
        try:
            tp = processor.ThresholdingProcessor(self.alarm_definition2)
        except Exception:
            tp = None
        self.assertIsInstance(tp, processor.ThresholdingProcessor)
        try:
            tp = processor.ThresholdingProcessor(self.alarm_definition3)
        except Exception:
            tp = None
        self.assertIsNone(tp)

    def test_process_alarms(self):
        tp = processor.ThresholdingProcessor(self.alarm_definition0)
        metrics_list = self.getMetric0()
        for metrics in metrics_list:
            tp.process_metrics(metrics)
        tp.process_alarms()
        self.assertEqual('ALARM', tp.expr_data_queue[None]['state'])

        tp = processor.ThresholdingProcessor(self.alarm_definition0)
        metrics_list = self.getMetric0()
        for metrics in metrics_list[0:2]:
            tp.process_metrics(metrics)
        tp.process_alarms()
        self.assertEqual('UNDETERMINED', tp.expr_data_queue[None]['state'])

        tp = processor.ThresholdingProcessor(self.alarm_definition2)
        metrics_list = self.getMetric2_0()
        for metrics in metrics_list:
            tp.process_metrics(metrics)
        tp.process_alarms()
        self.assertEqual('OK', tp.expr_data_queue[None]['state'])

        tp = processor.ThresholdingProcessor(self.alarm_definition2)
        metrics_list = self.getMetric2_1()
        for metrics in metrics_list:
            tp.process_metrics(metrics)
        tp.process_alarms()
        self.assertEqual('ALARM', tp.expr_data_queue[None]['state'])

        tp = processor.ThresholdingProcessor(self.alarm_definition1)
        metrics_list = self.getMetric1()
        for metrics in metrics_list:
            tp.process_metrics(metrics)
        alarms = tp.process_alarms()
        self.assertEqual(3, len(alarms))
        self.assertEqual('ALARM', tp.expr_data_queue['h1']['state'])
        self.assertEqual('ALARM', tp.expr_data_queue['h2']['state'])
