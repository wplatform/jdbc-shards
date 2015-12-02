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

import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaCommand {

    private Table table;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    private Expression defaultExpression;
    private Expression newSelectivity;
    private String addBefore;
    private String addAfter;
    private boolean ifNotExists;
    private ArrayList<Column> columnsToAdd;

    public AlterTableAlterColumn(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setOldColumn(Column oldColumn) {
        this.oldColumn = oldColumn;
    }

    public void setAddBefore(String before) {
        this.addBefore = before;
    }

    public void setAddAfter(String after) {
        this.addAfter = after;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setSelectivity(Expression selectivity) {
        newSelectivity = selectivity;
    }

    public void setDefaultExpression(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public void setNewColumn(Column newColumn) {
        this.newColumn = newColumn;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setNewColumns(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }

    public ArrayList<Column> getColumnsToAdd() {
        return columnsToAdd;
    }

    public void setColumnsToAdd(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }

    public Table getTable() {
        return table;
    }

    public Column getOldColumn() {
        return oldColumn;
    }

    public Column getNewColumn() {
        return newColumn;
    }

    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    public Expression getNewSelectivity() {
        return newSelectivity;
    }

    public String getAddBefore() {
        return addBefore;
    }

    public String getAddAfter() {
        return addAfter;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
    
    
    
}
