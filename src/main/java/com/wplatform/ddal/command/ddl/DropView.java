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
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statement
 * DROP VIEW
 */
public class DropView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;
    private int dropAction;

    public DropView(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                AlterTableAddConstraint.RESTRICT :
                    AlterTableAddConstraint.CASCADE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_VIEW;
    }

    public String getViewName() {
        return viewName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public int getDropAction() {
        return dropAction;
    }

}
