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
// Created on 2015年1月14日
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.io.Serializable;
import java.util.List;

import com.wplatform.ddal.dispatch.function.PartitionFunction;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RuleExpression implements Serializable {

    private static final long serialVersionUID = 1L;
    private final TableRouter tableRouter;
    private List<RuleColumn> ruleColumns;
    private String expression;
    private PartitionFunction function;

    /**
     * @param tableRouter
     */
    public RuleExpression(TableRouter tableRouter) {
        super();
        this.tableRouter = tableRouter;
    }

    /**
     * @return the ruleColumns
     */
    public List<RuleColumn> getRuleColumns() {
        return ruleColumns;
    }

    /**
     * @param ruleColumns the ruleColumns to set
     */
    public void setRuleColumns(List<RuleColumn> ruleColumns) {
        this.ruleColumns = ruleColumns;
    }

    /**
     * @return the expression
     */
    public String getExpression() {
        return expression;
    }

    /**
     * @param expression the expression to set
     */
    public void setExpression(String expression) {
        this.expression = expression;
    }

    /**
     * @return the tableRouter
     */
    public TableRouter getTableRouter() {
        return tableRouter;
    }
    /**
     * @return the function
     */
    public PartitionFunction getFunction() {
        return function;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RuleExpression that = (RuleExpression) o;

        if (ruleColumns != null ? !ruleColumns.equals(that.ruleColumns) : that.ruleColumns != null) return false;
        if (expression != null ? !expression.equals(that.expression) : that.expression != null) return false;
        return !(function != null ? !function.equals(that.function) : that.function != null);

    }

    @Override
    public int hashCode() {
        int result = ruleColumns != null ? ruleColumns.hashCode() : 0;
        result = 31 * result + (expression != null ? expression.hashCode() : 0);
        result = 31 * result + (function != null ? function.hashCode() : 0);
        return result;
    }
}
