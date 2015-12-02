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
package com.wplatform.ddal.dbobject;

import java.util.ArrayList;

import com.wplatform.ddal.command.Parser;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.Trace;

/**
 * The base class for all database objects.
 */
public abstract class DbObjectBase implements DbObject {

    /**
     * The database.
     */
    protected Database database;

    /**
     * The trace module.
     */
    protected Trace trace;

    /**
     * The comment (if set).
     */
    protected String comment;

    private int id;
    private String objectName;
    private boolean temporary;

    /**
     * Initialize some attributes of this object.
     *
     * @param db          the database
     * @param objectId    the object id
     * @param name        the name
     * @param traceModule the trace module name
     */
    protected void initDbObjectBase(Database db, int objectId, String name,
                                    String traceModule) {
        this.database = db;
        this.trace = db.getTrace(traceModule);
        this.id = objectId;
        this.objectName = name;
    }

    /**
     * Remove all dependent objects and free all resources (files, blocks in
     * files) of this object.
     *
     * @param session the session
     */
    @Override
    public abstract void removeChildrenAndResources(Session session);

    /**
     * Check if this object can be renamed. System objects may not be renamed.
     */
    @Override
    public abstract void checkRename();


    protected void setObjectName(String name) {
        objectName = name;
    }

    @Override
    public String getSQL() {
        return Parser.quoteIdentifier(objectName);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        return null;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return objectName;
    }

    /**
     * Set the main attributes to null to make sure the object is no longer
     * used.
     */
    protected void invalidate() {
        id = -1;
        database = null;
        trace = null;
        objectName = null;
    }

    @Override
    public void rename(String newName) {
        checkRename();
        objectName = newName;
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return objectName + ":" + id + ":" + super.toString();
    }

}
