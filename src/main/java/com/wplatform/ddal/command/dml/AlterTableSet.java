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
package com.wplatform.ddal.command.dml;

import com.wplatform.ddal.command.ddl.SchemaCommand;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE SET
 */
public class AlterTableSet extends SchemaCommand {

    private final int type;
    private final boolean value;
    private String tableName;
    private boolean checkExisting;

    public AlterTableSet(Session session, Schema schema, int type, boolean value) {
        super(session, schema);
        this.type = type;
        this.value = value;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return type;
    }

    public boolean isValue() {
        return value;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isCheckExisting() {
        return checkExisting;
    }

}
