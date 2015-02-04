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
// Created on 2014年4月20日
// $Id$

package com.suning.snfddal.route.rule;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableTopology implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Set<String>> topology;
    
    public TableTopology(Map<String, Set<String>> topology) {
        this.topology = topology;
    }

    public Set<String> getShard() {
        return Collections.unmodifiableSet(topology.keySet());
    }
    
    public Set<String> getTableInShard(String shardName) {
        Set<String> tables = topology.get(shardName);
        if(tables == null) {
            throw new IllegalArgumentException(shardName + " not existing.");
        }
        return Collections.unmodifiableSet(tables);
    }

    public String indexShard(int index) {
        Set<String> shards = topology.keySet();
        return index(index, shards);
    }
    
    public String indexTableInShard(String shardName, int index) {
        Set<String> tables = getTableInShard(shardName);
        return index(index, tables);
    }

    /**
     * @param index
     * @param set
     * @return
     */
    private String index(int index, Set<String> set) {
        String[] groups = set.toArray(new String[set.size()]);
        if (index < 0 || index >= groups.length) {
            String msg = "The index must be between 0 and " + (groups.length - 1);
            throw new ArrayIndexOutOfBoundsException(msg);
        }
        return groups[index];
    }

}
