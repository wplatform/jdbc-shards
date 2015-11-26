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
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statement ALTER TABLE RENAME
 */
public class AlterTableRename extends SchemaCommand {

    private Table oldTable;
    private String newTableName;
    private boolean hidden;

    public AlterTableRename(Session session, Schema schema) {
        super(session, schema);
    }

    public void setOldTable(Table table) {
        oldTable = table;
    }

    public void setNewTableName(String name) {
        newTableName = name;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_RENAME;
    }

    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    public Table getOldTable() {
        return oldTable;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public boolean isHidden() {
        return hidden;
    }
    
    

}
