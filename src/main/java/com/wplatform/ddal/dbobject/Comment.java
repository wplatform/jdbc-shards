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

import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.Trace;

/**
 * Represents a database object comment.
 */
public class Comment extends DbObjectBase {

    private final int objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        initDbObjectBase(database, id, getKey(obj), Trace.DATABASE);
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    private static String getTypeName(int type) {
        switch (type) {
            case DbObject.CONSTANT:
                return "CONSTANT";
            case DbObject.CONSTRAINT:
                return "CONSTRAINT";
            case DbObject.FUNCTION_ALIAS:
                return "ALIAS";
            case DbObject.INDEX:
                return "INDEX";
            case DbObject.ROLE:
                return "ROLE";
            case DbObject.SCHEMA:
                return "SCHEMA";
            case DbObject.SEQUENCE:
                return "SEQUENCE";
            case DbObject.TABLE_OR_VIEW:
                return "TABLE";
            case DbObject.TRIGGER:
                return "TRIGGER";
            case DbObject.USER:
                return "USER";
            case DbObject.USER_DATATYPE:
                return "DOMAIN";
            default:
                // not supported by parser, but required when trying to find a
                // comment
                return "type" + type;
        }
    }

    /**
     * Get the comment key name for the given database object. This key name is
     * used internally to associate the comment to the object.
     *
     * @param obj the object
     * @return the key name
     */
    public static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    @Override
    public int getType() {
        return DbObject.COMMENT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        //do nothing
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    /**
     * Set the comment text.
     *
     * @param comment the text
     */
    public void setCommentText(String comment) {
        this.commentText = comment;
    }

    public int getObjectType() {
        return objectType;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getCommentText() {
        return commentText;
    }
    
    

}
