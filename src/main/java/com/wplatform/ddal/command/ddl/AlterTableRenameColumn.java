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

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE ALTER COLUMN RENAME
 */
public class AlterTableRenameColumn extends DefineCommand {

    private Table table;
    private Column column;
    private String newName;

    public AlterTableRenameColumn(Session session) {
        super(session);
    }

    public Table getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public String getNewName() {
        return newName;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setNewColumnName(String newName) {
        this.newName = newName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_ALTER_COLUMN_RENAME;
    }

}
