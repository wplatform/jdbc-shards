/*
 * Copyright 2014 suning.com Holding Ltd.
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
// Created on 2014年3月25日
// $Id$

package com.suning.snfddal.route.rule;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableRouter implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private String id;
    
    private Map<String, Set<String>> partition;
        
    private RuleExpression shardRuleExpression;
    
    private RuleExpression tableRuleExpression;
    
    private TableTopology topology;

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
     * @return the partition
     */
    public Map<String, Set<String>> getPartition() {
        return partition;
    }

    /**
     * @param partition the partition to set
     */
    public void setPartition(Map<String, Set<String>> partition) {
        this.partition = partition;
    }

    /**
     * @return the ruleColumns
     */
    public List<RuleColumn> getRuleColumns() {
        Set<RuleColumn> temp = New.linkedHashSet();
        temp.addAll(shardRuleExpression.getRuleColumns());
        temp.addAll(tableRuleExpression.getRuleColumns());
        List<RuleColumn> result = New.arrayList(temp);
        return result;
    }
    /**
     * @return the shardRuleExpression
     */
    public RuleExpression getShardRuleExpression() {
        return shardRuleExpression;
    }

    /**
     * @param shardRuleExpression the shardRuleExpression to set
     */
    public void setShardRuleExpression(RuleExpression shardRuleExpression) {
        this.shardRuleExpression = shardRuleExpression;
    }

    /**
     * @return the tableRuleExpression
     */
    public RuleExpression getTableRuleExpression() {
        return tableRuleExpression;
    }

    /**
     * @param tableRuleExpression the tableRuleExpression to set
     */
    public void setTableRuleExpression(RuleExpression tableRuleExpression) {
        this.tableRuleExpression = tableRuleExpression;
    }

    /**
     * @return the topology
     */
    public TableTopology getTopology() {
        return topology;
    }
    
    /**
     * @param topology the topology to set
     */
    public void initTopology(String tableName) {
        Map<String, Set<String>> structure = New.linkedHashMap();
        for (Map.Entry<String, Set<String>> entry : partition.entrySet()) {
            String shardName = entry.getKey();
            Set<String> suffixs = entry.getValue();
            Set<String> tables = New.linkedHashSet();
            for (String suffix : suffixs) {
                tables.add(tableName + suffix);
            }
            structure.put(shardName, tables);
        }
        topology = new TableTopology(structure);
        
    }
    
    
    
}
