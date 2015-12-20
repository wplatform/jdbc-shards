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

package com.wplatform.ddal.route.rule;

import com.wplatform.ddal.route.algorithm.MultColumnPartitioner;
import com.wplatform.ddal.route.algorithm.Partitioner;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingCalculatorImpl implements RoutingCalculator {

    @Override
    public RoutingResult calculate(TableRouter tableRouter, RoutingArgument arg) {
        List<TableNode> partition = tableRouter.getPartition();
        Partitioner partitioner = tableRouter.getPartitioner();
        switch (arg.getArgumentType()) {
        case RoutingArgument.NONE_ROUTING_ARGUMENT:
            return RoutingResult.fixedResult(tableRouter.getPartition());
        case RoutingArgument.FIXED_ROUTING_ARGUMENT:
            List<Value> values = arg.getValues();
            Value[] toArray = values.toArray(new Value[values.size()]);
            Integer[] position = partitioner.partition(toArray);
            checkReturnValue(tableRouter, position);
            List<TableNode> selected = New.arrayList();
            for (Integer integer : position) {
                TableNode tableNode = partition.get(integer);
                selected.add(tableNode);
            }
            return RoutingResult.fixedResult(selected);
        case RoutingArgument.RANGE_ROUTING_ARGUMENT:
            Value start = arg.getStart();
            Value end = arg.getEnd();
            position = partitioner.partition(start, end);
            checkReturnValue(tableRouter, position);
            List<TableNode> seleced = New.arrayList();
            for (Integer integer : position) {
                TableNode tableNode = partition.get(integer);
                seleced.add(tableNode);
            }
            return RoutingResult.fixedResult(seleced);
        }
        return null;
    }

    @Override
    public RoutingResult calculate(TableRouter tableRouter, List<RoutingArgument> arguments) {
        List<TableNode> partition = tableRouter.getPartition();
        Partitioner partitioner = tableRouter.getPartitioner();
        boolean typeof = partitioner instanceof MultColumnPartitioner;
        if (!typeof) {
            String name = partitioner.getClass().getName();
            throw new RuleEvaluateException("Algorithm " + name + " can't supported multiple rule column.");
        }
        MultColumnPartitioner cp = (MultColumnPartitioner) partitioner;
        Integer[] position = cp.partition(arguments);
        checkReturnValue(tableRouter, position);
        List<TableNode> selected = New.arrayList();
        for (Integer integer : position) {
            TableNode tableNode = partition.get(integer);
            selected.add(tableNode);
        }
        return RoutingResult.fixedResult(selected);

    }

    /**
     * @param tableRouter
     * @param positions
     * @throws RuleEvaluateException
     */
    private void checkReturnValue(TableRouter tableRouter, Integer... positions) throws RuleEvaluateException {
        List<TableNode> partition = tableRouter.getPartition();
        String ptrName = tableRouter.getPartitioner().getClass().getName();
        if (positions == null) {
            String msg = String.format("The %s returned a illegal value null.", ptrName);
            throw new RuleEvaluateException(msg);
        }
        for (Integer position : positions) {
            if (position == null) {
                String msg = String.format("The %s returned a illegal value null.", ptrName);
                throw new RuleEvaluateException(msg);
            }
            if (position < 0 || position >= partition.size()) {
                String msg = String.format("The %s returned a illegal value %d, it's out of table nodes bounds.",
                        ptrName, position);
                throw new RuleEvaluateException(msg);
            }
        }
    }

}
