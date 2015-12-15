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

package com.wplatform.ddal.route.rule;

import java.io.Serializable;
import java.util.List;

import com.wplatform.ddal.route.algorithm.Partitioner;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableRouter implements Serializable {

    private static final long serialVersionUID = 1L;
    private String id;
    private List<TableNode> partition;
    private String algorithm;
    private List<String> ruleColumns;
    private Partitioner partitioner;

    /**
     * @param configuration
     */
    public TableRouter() {
        super();
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
    public List<String> getRuleColumns() {
        return ruleColumns;
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
     * @return the partitioner
     */
    public Partitioner getPartitioner() {
        return partitioner;
    }

    /**
     * @param partitioner the partitioner to set
     */
    public void setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
    }

    /**
     * @param ruleColumns the ruleColumns to set
     */
    public void setRuleColumns(List<String> ruleColumns) {
        this.ruleColumns = ruleColumns;
    }

    /**
     * @return the algorithm
     */
    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * @param algorithm the algorithm to set
     */
    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((partition == null) ? 0 : partition.hashCode());
        result = prime * result + ((partitioner == null) ? 0 : partitioner.hashCode());
        result = prime * result + ((ruleColumns == null) ? 0 : ruleColumns.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableRouter other = (TableRouter) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (partition == null) {
            if (other.partition != null)
                return false;
        } else if (!partition.equals(other.partition))
            return false;
        if (partitioner == null) {
            if (other.partitioner != null)
                return false;
        } else if (!partitioner.equals(other.partitioner))
            return false;
        if (ruleColumns == null) {
            if (other.ruleColumns != null)
                return false;
        } else if (!ruleColumns.equals(other.ruleColumns))
            return false;
        return true;
    }
    
    

}
