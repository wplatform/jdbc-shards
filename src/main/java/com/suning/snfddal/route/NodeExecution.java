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
// Created on 2015年1月28日
// $Id$

package com.suning.snfddal.route;

import java.util.ArrayList;
import java.util.List;

import com.suning.snfddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class NodeExecution {
    
    private final String shardName;
    private final String sql;
    private List<Value> params;
    private ArrayList<List<Value>> batchParam;
    private boolean batch;

    public NodeExecution(String shardName, String sql, List<Value> params) {
        super();
        this.shardName = shardName;
        this.sql = sql;
        this.params = params;
    }
    
    
    public NodeExecution(String shardName, String sql, ArrayList<List<Value>> batchParam) {
        super();
        this.shardName = shardName;
        this.sql = sql;
        this.batchParam = batchParam;
        this.batch = true;
    }

    /**
     * @return the shardName
     */
    public String getShardName() {
        return shardName;
    }

    /**
     * @return the sql
     */
    public String getSql() {
        return sql;
    }

    /**
     * @return the params
     */
    public List<Value> getParams() {
        return params;
    }


    /**
     * @return the batchParam
     */
    public ArrayList<List<Value>> getBatchParam() {
        return batchParam;
    }

    /**
     * @return the batch
     */
    public boolean isBatch() {
        return batch;
    }
    
    

}
