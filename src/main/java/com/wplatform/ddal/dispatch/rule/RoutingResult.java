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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.wplatform.ddal.dispatch.TableRoutingException;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<TableNode> all;

    private List<TableNode> selected;

    RoutingResult(List<TableNode> all, List<TableNode> selected) {
        if (selected.isEmpty()) {
            throw new TableRoutingException("Empty table node.");
        }
        if (selected.size() > all.size()) {
            throw new IllegalArgumentException();
        }
        this.all = selected;
        this.selected = selected;
    }


    public static RoutingResult fixedResult(TableNode tableNode) {
        List<TableNode> nodes = New.arrayList(1);
        nodes.add(tableNode);
        return new RoutingResult(nodes, nodes);
    }

    public static RoutingResult fixedResult(List<TableNode> nodes) {
        return new RoutingResult(nodes, nodes);
    }

    public static RoutingResult fixedResult(TableNode[] tableNode) {
        List<TableNode> nodes = Arrays.asList(tableNode);
        return new RoutingResult(nodes, nodes);
    }

    public boolean isMultipleNode() {
        return selected.size() > 1;
    }

    public TableNode getSingleResult() {
        if (isMultipleNode()) {
            throw new IllegalStateException("The RoutingResult has multiple table node.");
        }
        return selected.get(0);
    }

    public TableNode[] getSelectNodes() {
        return selected.toArray(new TableNode[selected.size()]);
    }

    public boolean isFullNode() {
        return all.equals(selected) && all.size() > 1;
    }

    public int tableNodeCount() {
        return selected.size();
    }

    public TableNode[] group() {
        List<TableNode> result;
        if (isMultipleNode()) {
            Set<String> shards = shardNames();
            result = New.arrayList(shards.size());
            for (String shardName : shardNames()) {
                List<String> tables = New.arrayList();
                List<String> suffixes = New.arrayList();
                for (TableNode tableNode : selected) {
                    String nodeName = tableNode.getShardName();
                    String tableName = tableNode.getObjectName();
                    String suffix = tableNode.getSuffix();
                    if (shardName.equals(nodeName)) {
                        tables.add(tableName);
                        suffixes.add(suffix);
                    }
                }
                TableNode tableNode;
                if (tables.size() > 1) {
                    String[] t = tables.toArray(new String[tables.size()]);
                    String[] s = suffixes.toArray(new String[suffixes.size()]);
                    tableNode = new GroupTableNode(shardName, t, s);
                } else {
                    tableNode = new TableNode(shardName, tables.get(0), suffixes.get(0));
                }
                result.add(tableNode);
            }
        } else {
            result = selected;
        }
        return result.toArray(new TableNode[result.size()]);
    }

    private Set<String> shardNames() {
        Set<String> shards = New.linkedHashSet();
        for (TableNode tableNode : selected) {
            String shardName = tableNode.getShardName();
            shards.add(shardName);
        }
        return shards;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((selected == null) ? 0 : selected.hashCode());
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
        RoutingResult other = (RoutingResult) obj;
        if (selected == null) {
            if (other.selected != null)
                return false;
        } else if (!selected.equals(other.selected))
            return false;
        return true;
    }

}
