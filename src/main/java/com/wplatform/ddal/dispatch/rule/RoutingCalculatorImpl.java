/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2014年4月24日
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingCalculatorImpl implements RoutingCalculator {

    private RuleEvaluator evaluator = new OgnlRuleEvaluator();

    public RuleEvaluator getEvaluator() {
        return evaluator;
    }

    public void setEvaluator(RuleEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    @Override
    public RoutingResult calculate(TableRouter tableRouter, Map<String, List<Value>> columnValue) {
        if (tableRouter == null) {
            throw new IllegalArgumentException("tableRule is null.");
        }
        if (columnValue == null) {
            throw new IllegalArgumentException("columnValue is null.");
        }

        RuleExpression expression = tableRouter.getRuleExpression();

        List<TableNode> tableNode = null;
        if (canUseRule(expression, columnValue)) {
            tableNode = evaluateTableRule(tableRouter, columnValue);
        } else {
            // 无库规则,库的范围是TableRule配置的所有库
            tableNode = tableRouter.getPartition();
        }
        return new RoutingResult(tableRouter.getPartition(), tableNode);
    }

    /**
     * @param ruleToUse
     * @param columnValue
     * @param tableRule
     */
    private List<TableNode> evaluateTableRule(TableRouter tr, Map<String, List<Value>> args) {
        List<TableNode> result = New.arrayList();
        RuleExpression rule = tr.getRuleExpression();
        List<TableNode> partion = tr.getPartition();
        List<RuleColumn> ruleColumns = rule.getRuleColumns();
        Map<String, List<Value>> paramCollections = New.hashMap(ruleColumns.size(), 1L);
        for (RuleColumn ruleColumn : ruleColumns) {
            String name = ruleColumn.getName();
            paramCollections.put(name, args.get(name));
        }
        // 一个规则存在多个RuleColumn，多个RuleColumn对应的取值集合做笛卡尔积后的所有集
        for (Map<String, Value> parameters : new CrossedCollection(paramCollections)) {
            TableNode tableNode = null;
            Object evlValue = evaluator.evaluate(rule, parameters);
            if (evlValue == null) {
                throw new RuleEvaluateException("The rule expression " + rule.getExpression()
                        + " evaluate a null value.");
            }
            if (evlValue instanceof TableNode) {
                if (!partion.contains(evlValue)) {
                    throw new RuleEvaluateException("The rule expression " + rule.getExpression() + " evaluated "
                            + evlValue + " is not in partition list.");
                }
                tableNode = (TableNode) evlValue;
            } else if (evlValue.getClass() == int.class || evlValue.getClass() == Integer.class
                    || evlValue.getClass() == long.class || evlValue.getClass() == Long.class
                    || evlValue.getClass() == short.class || evlValue.getClass() == Short.class
                    || evlValue.getClass() == byte.class || evlValue.getClass() == Byte.class) {
                try {
                    int index = Integer.parseInt(evlValue.toString());
                    tableNode = partion.get(index);
                } catch (IndexOutOfBoundsException e) {
                    throw new RuleEvaluateException("The rule expression " + rule.getExpression() + " evaluated "
                            + evlValue + " is out of range partition list.");
                }

            } else {
                throw new RuleEvaluateException("The group rule expression " + rule.getExpression()
                        + " return a value " + evlValue.getClass() + " which type is unsupported.");
            }
            result.add(tableNode);
        }
        return result;
    }

    /**
     * 对于分库分表存在多个Rule的情况下，choiceRule负责根据表的字段值选取一个符合条件的Rule做为sharding规则，
     * 先择的规则按优先顺序，优先最大匹配，先匹配所有列，找不到再去除可选列之后匹配
     *
     * @param rules
     * @param columnValue
     * @return
     */
    private boolean canUseRule(RuleExpression rule, Map<String, List<Value>> columnValue) {
        if (rule == null) {
            return false;
        }
        // 完全匹配所有列
        List<RuleColumn> ruleColumns = rule.getRuleColumns();
        for (RuleColumn ruleColumn : ruleColumns) {
            List<Value> values = columnValue.get(ruleColumn.getName());
            if (values == null || values.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将列的值域通过笛卡尔积运算，转化为参数一一对应的值域
     * <p>
     * <p>
     * <pre>
     * 如输入参数： {
     *        column1:{ 1, 2, 3 },
     *        column2:{ a, b, c, d }
     * }
     * </pre>
     * <p>
     * <p>
     * <pre>
     * 输出结果：{
     *      {column1=1, column2=a}
     *      {column1=1, column2=b}
     *      {column1=1, column2=c}
     *      {column1=2, column2=a}
     *      {column1=2, column2=b}
     *      {column1=2, column2=c}
     * }
     * </pre>
     *
     * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
     */
    private static class CrossedCollection implements Iterable<Map<String, Value>> {

        private Map<String, List<Value>> collection;

        private List<Map<String, Value>> crossedResult;

        /**
         * @param collection
         */
        private CrossedCollection(Map<String, List<Value>> collection) {
            this.collection = collection;
        }

        /* (non-Javadoc)
         * @see java.lang.Iterable#iterator() */
        @Override
        public Iterator<Map<String, Value>> iterator() {
            crossedResult = cross(this.collection);
            return crossedResult.iterator();
        }

        private List<Map<String, Value>> cross(Map<String, List<Value>> crossArgs) {
            // Set是无顺的且不能按顺号迭代，先将Set转为List
            Map<String, List<Value>> crossSource = New.hashMap(crossArgs.size(), 1L);
            List<String> columnNames = new ArrayList<String>(crossArgs.keySet());
            // 计算出笛卡尔积行数
            int rows = columnNames.size() > 0 ? 1 : 0;
            for (String column : columnNames) {
                crossSource.put(column, new ArrayList<Value>(crossArgs.get(column)));
                rows *= crossArgs.get(column).size();
            }
            // 笛卡尔积索引记录
            int[] record = new int[columnNames.size()];
            List<Map<String, Value>> results = new ArrayList<Map<String, Value>>();
            // 产生笛卡尔积
            for (int i = 0; i < rows; i++) {
                // List<String> row = new ArrayList<String>();
                Map<String, Value> row = New.linkedHashMap(record.length, 1L);
                // 生成笛卡尔积的每组数据
                for (int index = 0; index < record.length; index++) {
                    String columnName = columnNames.get(index);
                    List<Value> columnValues = crossSource.get(columnName);
                    row.put(columnName, columnValues.get(record[index]));
                }
                results.add(row);
                crossRecord(columnNames, crossSource, record, crossArgs.size() - 1);
            }
            return results;
        }

        /**
         * 产生笛卡尔积当前行索引记录.
         *
         * @param sourceArgs 要产生笛卡尔积的源数据
         * @param record     每行笛卡尔积的索引组合
         * @param level      索引组合的当前计算层级
         */
        private void crossRecord(List<String> columnNames, Map<String, List<Value>> crossSource, int[] record, int level) {
            record[level] = record[level] + 1;
            if (record[level] >= crossSource.get(columnNames.get(level)).size() && level > 0) {
                record[level] = 0;
                crossRecord(columnNames, crossSource, record, level - 1);
            }
        }

    }

}
