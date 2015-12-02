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

import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;

/**
 * A database object such as a table, an index, or a user.
 */
public interface DbObject {

    /**
     * The object is of the type table or view.
     */
    int TABLE_OR_VIEW = 0;

    /**
     * This object is an index.
     */
    int INDEX = 1;

    /**
     * This object is a user.
     */
    int USER = 2;

    /**
     * This object is a sequence.
     */
    int SEQUENCE = 3;

    /**
     * This object is a trigger.
     */
    int TRIGGER = 4;

    /**
     * This object is a constraint (check constraint, unique constraint, or
     * referential constraint).
     */
    int CONSTRAINT = 5;

    /**
     * This object is a setting.
     */
    int SETTING = 6;

    /**
     * This object is a role.
     */
    int ROLE = 7;

    /**
     * This object is a right.
     */
    int RIGHT = 8;

    /**
     * This object is an alias for a Java function.
     */
    int FUNCTION_ALIAS = 9;

    /**
     * This object is a schema.
     */
    int SCHEMA = 10;

    /**
     * This object is a constant.
     */
    int CONSTANT = 11;

    /**
     * This object is a user data type (domain).
     */
    int USER_DATATYPE = 12;

    /**
     * This object is a comment.
     */
    int COMMENT = 13;

    /**
     * This object is a user-defined aggregate function.
     */
    int AGGREGATE = 14;

    /**
     * Get the SQL name of this object (may be quoted).
     *
     * @return the SQL name
     */
    String getSQL();

    /**
     * Get the list of dependent children (for tables, this includes indexes and
     * so on).
     *
     * @return the list of children
     */
    ArrayList<DbObject> getChildren();

    /**
     * Get the database.
     *
     * @return the database
     */
    Database getDatabase();

    /**
     * Get the unique object id.
     *
     * @return the object id
     */
    int getId();

    /**
     * Get the name.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the object type.
     *
     * @return the object type
     */
    int getType();

    /**
     * Delete all dependent children objects and resources of this object.
     *
     * @param session the session
     */
    void removeChildrenAndResources(Session session);

    /**
     * Check if renaming is allowed. Does nothing when allowed.
     */
    void checkRename();

    /**
     * Rename the object.
     *
     * @param newName the new name
     */
    void rename(String newName);

    /**
     * Check if this object is temporary (for example, a temporary table).
     *
     * @return true if is temporary
     */
    boolean isTemporary();

    /**
     * Tell this object that it is temporary or not.
     *
     * @param temporary the new value
     */
    void setTemporary(boolean temporary);

    /**
     * Get the current comment of this object.
     *
     * @return the comment, or null if not set
     */
    String getComment();

    /**
     * Change the comment of this object.
     *
     * @param comment the new comment, or null for no comment
     */
    void setComment(String comment);

}
