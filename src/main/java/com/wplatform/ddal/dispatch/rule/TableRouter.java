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
// Created on 2014年3月25日
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableRouter implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Configuration configuration;
    private String id;
    private List<TableNode> partition;
    private RuleExpression ruleExpression;

    /**
     * @param configuration
     */
    public TableRouter(Configuration configuration) {
        super();
        this.configuration = configuration;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the ruleColumns
     */
    public List<RuleColumn> getRuleColumns() {
        Set<RuleColumn> temp = New.linkedHashSet();
        temp.addAll(ruleExpression.getRuleColumns());
        List<RuleColumn> result = New.arrayList(temp);
        return result;
    }

    /**
     * @return the partition
     */
    public List<TableNode> getPartition() {
        return partition;
    }

    /**
     * @param partition the partition to set
     */
    public void setPartition(List<TableNode> partition) {
        this.partition = partition;
    }

    /**
     * @return the ruleExpression
     */
    public RuleExpression getRuleExpression() {
        return ruleExpression;
    }

    /**
     * @param ruleExpression the ruleExpression to set
     */
    public void setRuleExpression(RuleExpression ruleExpression) {
        this.ruleExpression = ruleExpression;
    }

    /**
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableRouter that = (TableRouter) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return !(ruleExpression != null ? !ruleExpression.equals(that.ruleExpression) : that.ruleExpression != null);

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (ruleExpression != null ? ruleExpression.hashCode() : 0);
        return result;
    }
}
