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


def calc_value(func, data_list):
    """Calc float values according to 5 functions."""
    if len(data_list) == 0:
        return None
    value = 0.0
    if func == 'SUM':
        value = sum(data_list)
    elif func == 'AVG':
        value = sum(data_list)
        value /= len(data_list)
    elif func == 'MAX':
        value = max(data_list)
    elif func == 'MIN':
        value = min(data_list)
    elif func == 'COUNT':
        value = len(data_list)
    else:
        value = None
    return value


def compare_thresh(values, op, thresh):
    """Check if value from metrics exceeds thresh."""
    state = 'OK'
    for value in values:
        if value:
            if op == 'GT' and value <= thresh:
                return state
            elif op == 'LT' and thresh <= value:
                return state
            elif op == 'LTE' and value > thresh:
                return state
            elif op == 'GTE' and thresh > value:
                return state
    state = 'ALARM'
    for value in values:
        if value is None:
            state = 'UNDETERMINED'
    return state


def calc_logic(logic_operator, subs):
    """Calc overall state of an alarm expression."""
    if logic_operator == 'AND':
        for o in subs:
            if o == 'OK':
                return 'OK'
        for o in subs:
            if o == 'UNDETERMINED':
                return 'UNDETERMINED'
        return 'ALARM'
    elif logic_operator == 'OR':
        for o in subs:
            if o == 'ALARM':
                return 'ALARM'
        for o in subs:
            if o == 'UNDETERMINED':
                return 'UNDETERMINED'
        return 'OK'
    else:
        return 'UNDETERMINED'
