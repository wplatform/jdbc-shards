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

import com.wplatform.ddal.route.algorithm.Partitioner;

import java.io.Serializable;
import java.util.List;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableRouter that = (TableRouter) o;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (algorithm != null ? !algorithm.equals(that.algorithm) : that.algorithm != null) return false;
        return ruleColumns != null ? ruleColumns.equals(that.ruleColumns) : that.ruleColumns == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (algorithm != null ? algorithm.hashCode() : 0);
        result = 31 * result + (ruleColumns != null ? ruleColumns.hashCode() : 0);
        return result;
    }
}
