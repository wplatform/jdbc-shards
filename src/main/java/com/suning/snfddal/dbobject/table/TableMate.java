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
// Created on 2014年12月25日
// $Id$
package com.suning.snfddal.dbobject.table;

import java.util.ArrayList;
import java.util.List;

import com.suning.snfddal.command.ddl.CreateTableData;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.index.IndexMate;
import com.suning.snfddal.dbobject.index.IndexType;
import com.suning.snfddal.dispatch.rule.RuleColumn;
import com.suning.snfddal.dispatch.rule.TableNode;
import com.suning.snfddal.dispatch.rule.TableRouter;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableMate extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100000;
    
    private final boolean globalTemporary;
    private final ArrayList<Index> indexes = New.arrayList();
    private Index scanIndex;
    
    
    private TableRouter tableRouter;
    private TableNode matedataNode;
    private TableNode[] broadcastNode;
    private int scanLevel;
    
    public TableMate(CreateTableData data) {
        super(data.schema, data.id, data.tableName);
        this.globalTemporary = data.globalTemporary;
        setTemporary(data.temporary);
        Column[] cols = new Column[data.columns.size()];
        data.columns.toArray(cols);
        setColumns(cols);
        scanIndex = new IndexMate(this, data.id, null, IndexColumn.wrap(cols), IndexType.createScan());
        indexes.add(scanIndex);
    }
    
    /**
     * @return the scanIndex
     */
    public Index getScanIndex() {
        return scanIndex;
    }

    /**
     * @param scanIndex the scanIndex to set
     */
    public void setScanIndex(Index scanIndex) {
        this.scanIndex = scanIndex;
    }

    /**
     * @return the tableRouter
     */
    public TableRouter getTableRouter() {
        return tableRouter;
    }

    /**
     * @param tableRouter the tableRouter to set
     */
    public void setTableRouter(TableRouter tableRouter) {
        this.tableRouter = tableRouter;
    }

    /**
     * @return the scanLevel
     */
    public int getScanLevel() {
        return scanLevel;
    }

    /**
     * @param scanLevel the scanLevel to set
     */
    public void setScanLevel(int scanLevel) {
        this.scanLevel = scanLevel;
    }
    
    /**
     * @return the matedataNode
     */
    public TableNode getMatedataNode() {
        return matedataNode;
    }

    /**
     * @param matedataNode the matedataNode to set
     */
    public void setMatedataNode(TableNode matedataNode) {
        this.matedataNode = matedataNode;
    }

    /**
     * @return the broadcastNode
     */
    public TableNode[] getBroadcastNode() {
        return broadcastNode;
    }

    /**
     * @param broadcastNode the broadcastNode to set
     */
    public void setBroadcastNode(TableNode[] broadcastNode) {
        this.broadcastNode = broadcastNode;
    }
    
    /**
     * @return
     * @see com.suning.snfddal.dispatch.rule.TableRouter#getPartition()
     */
    public TableNode[] getPartitionNode() {
        if(tableRouter != null) {
            List<TableNode> partition = tableRouter.getPartition();
            return partition.toArray(new TableNode[partition.size()]);
        }
        if(broadcastNode != null) {
            return broadcastNode;
        }
        return new TableNode[]{matedataNode};
        
    }
    /**
     * validation the rule columns is in the table columns
     * @param config
     * @param columns
     */
    public void validationRuleColumn() {
        if(isLoadFailed()) {
           return; 
        }
        if(tableRouter != null) {
            for (RuleColumn ruleCol : tableRouter.getRuleColumns()) {
                Column matched = null;
                for (Column column : columns) {
                    String colName = column.getName();
                    if(colName.equalsIgnoreCase(ruleCol.getName())) {
                        matched = column;
                        break;
                    }                
                }
                if(matched == null){
                    throw DbException.throwInternalError("The rule column " + ruleCol
                            + " does not exist in "+ getName() + " table." );
                }
            }
        }
    }


    /**
     * @return the loadFailed
     */
    public boolean isLoadFailed() {
        return this.columns.length == 0;
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }
    
    @Override
    public Index addIndex(ArrayList<Column> list, IndexType indexType) {
        Column[] cols = new Column[list.size()];
        list.toArray(cols);
        Index index = new IndexMate(this, 0, null, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
        return index;
   }

    @Override
    public String getTableType() {
        return Table.TABLE;
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.dbobject.table.Table#isDeterministic() */
    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        if(this.tableRouter == null) {
            return ROW_COUNT_APPROXIMATION;
        } else {
            return tableRouter.getPartition().size() * ROW_COUNT_APPROXIMATION;
        }
        
    }

    @Override
    public void checkRename() {

    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    

}
