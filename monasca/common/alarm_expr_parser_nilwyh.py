#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2014 Hewlett-Packard
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
import itertools
import sys
import collections

import pyparsing


def compress_stack(stack):
    if len(stack) <= 1:
        return
    if stack[-1] == ')':
        stack.pop()
        temp = stack.pop()
        stack.pop()
        stack.append(temp)
        compress_stack(stack)
        return
    if stack[-1] == '(':
        return

    temp2 = stack.pop()
    if stack[-1] == '(':
        stack.append(temp2)
        return

    tempo = stack.pop()
    temp1 = stack.pop()

    temp_new = {}
    temp_new['left'] = temp1
    temp_new['right'] = temp2
    temp_new['value'] = tempo

    stack.append(temp_new)
    compress_stack(stack)
    return


class AlarmExprParser(object):
    def __init__(self, expr):
        self._expr = '(' + expr + ')'

        self.sub_expr_list = []
        self.logic_tree = None
        self.build_logic_tree(expr)


    def build_logic_tree(self, expr):
        expr = self._expr
        print expr
        size = len(expr)
        stack = []
        i = 0
        pre = 0
        new_expr = True
        while i < size:
            if new_expr and expr[i] == '(':
                stack.append('(')
                pre = i+1
            elif expr[i] == ')':
                if not new_expr:
                    new_expr = True
                else:
                    if pre > -1:
                        stack.append(self.build_sub_expr(expr[pre:i]))
                        compress_stack(stack)
                    stack.append(')')
                    compress_stack(stack)
                    pre = -1
            elif new_expr:
                if i+3 <= size and expr[i:i+3] == 'min':
                    new_expr = False
                elif i+3 <= size and expr[i:i+3] == 'max':
                    new_expr = False
                elif i+3 <= size and expr[i:i+3] == 'avg':
                    new_expr = False
                elif i+3 <= size and expr[i:i+3] == 'sum':
                    new_expr = False
                elif i+5 <= size and expr[i:i+5] == 'count':
                    new_expr = False
                elif i+3 <= size and expr[i:i+3] == 'and':
                    stack.append(self.build_sub_expr(expr[pre:i]))
                    compress_stack(stack)
                    stack.append('and')
                    i += 2
                    pre = i+1
                elif i+2 <= size and expr[i:i+2] == 'or':
                    stack.append(self.build_sub_expr(expr[pre:i]))
                    compress_stack(stack)
                    stack.append('or')
                    i += 1
                    pre = i+1
            i += 1
        self.logic_tree = stack.pop()

    def build_sub_expr(self, expr):
        sub_expr = {}
        expr = expr.strip()
        i = 0
        expr_function = ''
        if expr[i:i + 3] == 'min':
            expr_function = 'min'
            i += 3
        elif expr[i:i + 3] == 'max':
            expr_function = 'max'
            i += 3
        elif expr[i:i + 3] == 'avg':
            expr_function = 'avg'
            i += 3
        elif expr[i:i + 3] == 'sum':
            expr_function = 'sum'
            i += 3
        elif expr[i:i + 5] == 'count':
            expr_function = 'count'
            i += 5

        size = len(expr)
        lp = 0
        lt = 0
        lq = 0
        expr_name = ''
        expr_dimensions = {}
        cur_d = ''
        expr_operator = ''
        expr_thresh = 0.0
        expr_period = 60
        while i < size:
            if expr[i] == '(':
                lp = i
            elif expr[i] == '{':
                lt = i
                expr_name = expr[lp+1:lt].strip()
                i += 1
                pre = i
                while expr[i] != '}':
                    if expr[i] == '=':
                        cur_d = expr[pre:i].strip()
                        pre = i+1
                    elif expr[i] == ',':
                        expr_dimensions[cur_d] = expr[pre:i].strip()
                        pre = i+1
                    i += 1
                expr_dimensions[cur_d] = expr[pre:i].strip()
            elif expr[i] == ',':
                lq = i+1
                if lt == 0:
                    expr_name = expr[lp+1:i].strip()
            elif expr[i] == ')':
                if lq == 0 and lt == 0:
                    expr_name = expr[lp+1:i].strip()
                elif lq != 0:
                    expr_period = int(expr[lq:i])
            else:
                if expr[i:i+2] == '>=':
                    expr_operator = 'GTE'
                    i += 2
                    expr_thresh = float(expr[i:size])
                    break
                elif expr[i:i+2] == '<=':
                    expr_operator = 'LTE'
                    i += 2
                    expr_thresh = float(expr[i:size])
                    break
                elif expr[i:i+1] == '>':
                    expr_operator = 'GT'
                    i += 1
                    expr_thresh = float(expr[i:size])
                    break
                elif expr[i:i+2] == '<':
                    expr_operator = 'LT'
                    i += 1
                    expr_thresh = float(expr[i:size])
                    break
            i += 1
        sub_expr['period'] = expr_period
        sub_expr['function'] = expr_function
        sub_expr['dimensions'] = expr_dimensions
        sub_expr['operator'] = expr_operator
        sub_expr['metrics_name'] = expr_name
        sub_expr['threshold'] = expr_thresh
        sub_expr['state'] = 'UNDETERMINED'
        sub_expr['data_list'] = collections.deque(maxlen=256)
        self.sub_expr_list.append(sub_expr)
        ans = {}
        ans['value'] = sub_expr
        ans['left'] = None
        ans['right'] = None
        return sub_expr

    def get_logic_tree(self):
        return self.logic_tree

    def get_sub_expr_list(self):
        return self.sub_expr_list


def main():
    """Used for development and testing."""

    expr1 = ("max(foo{hostname=mini-mon,千幸福的笑脸घ=千幸福的笑脸घ}, 120) > 1100 and (max(bar { asd = asd} )>1200 "
             " or max(biz)>1300)".decode('utf8'))

    expr2 = "max(foo)>=100"

    for expr in (expr1,expr2):
        print ('orig expr: {}'.format(expr.encode('utf8')))
        alarmExprParser = AlarmExprParser(expr)
        sub_expr = alarmExprParser.get_sub_expr_list()

        for sub_expression in sub_expr:
            print sub_expression
            print()
        print 'logic tree:'
        print alarmExprParser.get_logic_tree()
        print 'end'


if __name__ == "__main__":
    sys.exit(main())
