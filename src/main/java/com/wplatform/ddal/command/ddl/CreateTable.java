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
package com.wplatform.ddal.command.ddl;

import java.util.ArrayList;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;

/**
 * This class represents the statement CREATE TABLE
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateTable extends SchemaCommand {

    private final CreateTableData data = new CreateTableData();
    private final ArrayList<DefineCommand> constraintCommands = New.arrayList();
    private IndexColumn[] pkColumns;
    private boolean ifNotExists;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private boolean sortedInsertMode;
    private String charset;

    public CreateTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }
    
    public Query getQuery() {
        return this.asQuery;
    }

    public void setTemporary(boolean temporary) {
        data.temporary = temporary;
    }
    
    public boolean isTemporary() {
        return data.temporary;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }
    
    public String getTableName() {
        return data.tableName;
    }
    
    public int getColumnCount() {
        return data.columns.size();
    }

    /**
     * Add a column to this table.
     *
     * @param column the column to add
     */
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    /**
     * Add a constraint statement to this statement. The primary key definition
     * is one possible constraint statement.
     *
     * @param command the statement to add
     */
    public void addConstraintCommand(DefineCommand command) {
        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }
    
    /**
     * @return constraintCommands
     */
    public ArrayList<Column> getColumns() {
        return data.columns;
    }
    
    /**
     * @return constraintCommands
     */
    public ArrayList<DefineCommand> getConstraintCommands() {
        return constraintCommands;
    }

    /**
     * set the ifNotExists
     * @param ifNotExists
     */
    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
    
    /**
     * @return the ifNotExists
     */
    public boolean isIfNotExists() {
        return ifNotExists;
    }

    /**
     * Sets the primary key columns, but also check if a primary key with
     * different columns is already defined.
     *
     * @param columns the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
    }
    
    public boolean isGlobalTemporary() {
        return data.globalTemporary;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    public void setTableEngine(String tableEngine) {
        data.tableEngine = tableEngine;
    }
    
    public String getTableEngine() {
        return data.tableEngine;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        data.tableEngineParams = tableEngineParams;
    }
    
    public ArrayList<String> getTableEngineParams() {
        return data.tableEngineParams;
    }

    public void setHidden(boolean isHidden) {
        data.isHidden = isHidden;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TABLE;
    }

    @Override
    public String getPlanSQL() {
        return null;
    }

    /**
     * @return the onCommitDrop
     */
    public boolean isOnCommitDrop() {
        return onCommitDrop;
    }

    /**
     * @return the onCommitTruncate
     */
    public boolean isOnCommitTruncate() {
        return onCommitTruncate;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @return the charset
     */
    public String getCharset() {
        return charset;
    }

    /**
     * @return the sortedInsertMode
     */
    public boolean isSortedInsertMode() {
        return sortedInsertMode;
    }

    
    

}
