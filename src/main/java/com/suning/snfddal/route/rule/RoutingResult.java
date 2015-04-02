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
import java.util.Set;

import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<TableNode> all;

    private List<TableNode> selected;

    RoutingResult(List<TableNode> all, List<TableNode> selected) {
        this.all = selected;
        this.selected = selected;
    }
    
    
    public static RoutingResult createResult(TableNode tableNode) {
        List<TableNode> nodes = New.arrayList(1);
        nodes.add(tableNode);
        return new RoutingResult(nodes,nodes);
    }

    public boolean isMultipleNode() {
        return selected.size() > 1;
    }

    public TableNode singleResult() {
        if (isMultipleNode()) {
            throw new IllegalStateException("The RoutingResult has multiple table node.");
        }
        return selected.get(0);
    }

    public boolean isFullNode() {
        return all.equals(selected) && all.size() > 1;
    }

    public int tableNodeCount() {
        return selected.size();
    }

    public Set<String> shardNames() {
        Set<String> shards = New.linkedHashSet();
        for (TableNode tableNode : selected) {
            String shardName = tableNode.getShardName();
            shards.add(shardName);
        }
        return shards;
    }

    public Set<String> tableNames(String shardName) {
        Set<String> collect = New.linkedHashSet();
        for (TableNode tableNode : selected) {
            String nodeName = tableNode.getShardName();
            String tableName = tableNode.getTableName();
            if (shardName.equals(nodeName)) {
                collect.add(tableName);
            }
        }
        return collect;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode() */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((selected == null) ? 0 : selected.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object) */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RoutingResult other = (RoutingResult) obj;
        if (selected == null) {
            if (other.selected != null)
                return false;
        } else if (!selected.equals(other.selected))
            return false;
        return true;
    }

}
