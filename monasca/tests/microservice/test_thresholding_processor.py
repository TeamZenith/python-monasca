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


from monasca.microservice import thresholding_processor


def getTestAlarmDef():
    """give alarm definition."""
    items = {"id": "yqwyeasidiaisidsaiidsa",
        "name": "Average CPU percent greater than 10",
   "description":"The average CPU percent is greater than 10",
   "expression": "max(foo{hostname=mini-mon,mu=na}, 120) > 1100 and max(bar { asd = asd} )>1200 or avg(biz)>1300",
   "match_by":[
     "hostname"
   ],
   "severity":"LOW",
   "ok_actions":[
     "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
   ],
   "alarm_actions":[
     "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
   ],
   "undetermined_actions":[
     "c60ec47e-5038-4bf1-9f95-4046c6e9a759"
   ]}
    return items

def produceMetric(v):
    items = {"name":"biz",
   "dimensions":{
      "key1":"value1",
      "key2":"value2"
   },
   "timestamp":time.time(),
   "value":v}
    return items


def main():
    """Used for development and testing."""
    test_alarm_def = json.dumps(getTestAlarmDef())
    print test_alarm_def
    t_thresholding_processor = thresholding_processor.ThresholdingProcessor(test_alarm_def)
    print t_thresholding_processor.expression
    print t_thresholding_processor.logic_tree['left']
    print t_thresholding_processor.sub_expression
    print {}
    metrics1 = json.dumps(produceMetric(1500))
    metrics2 = json.dumps(produceMetric(500))

    print t_thresholding_processor.process_msg(metrics1)
    print t_thresholding_processor.process_msg(metrics2)
    print t_thresholding_processor.sub_expression[2]


if __name__ == "__main__":
    main()
