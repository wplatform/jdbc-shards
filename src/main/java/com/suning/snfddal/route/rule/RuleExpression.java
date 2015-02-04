/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年1月14日
// $Id$

package com.suning.snfddal.route.rule;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RuleExpression implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private List<RuleColumn> ruleColumns;

    private String expression;

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
    
    
}
