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

import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.Trace;

/**
 * An access right. Rights are regular database objects, but have generated
 * names.
 */
public class Right extends DbObjectBase {

    /**
     * The right bit mask that means: selecting from a table is allowed.
     */
    public static final int SELECT = 1;

    /**
     * The right bit mask that means: deleting rows from a table is allowed.
     */
    public static final int DELETE = 2;

    /**
     * The right bit mask that means: inserting rows into a table is allowed.
     */
    public static final int INSERT = 4;

    /**
     * The right bit mask that means: updating data is allowed.
     */
    public static final int UPDATE = 8;

    /**
     * The right bit mask that means: create/alter/drop schema is allowed.
     */
    public static final int ALTER_ANY_SCHEMA = 16;

    /**
     * The right bit mask that means: select, insert, update, delete, and update
     * for this object is allowed.
     */
    public static final int ALL = SELECT | DELETE | INSERT | UPDATE;

    private Role grantedRole;
    private int grantedRight;
    private Table grantedTable;
    private RightOwner grantee;

    public Right(Database db, int id, RightOwner grantee, Role grantedRole) {
        initDbObjectBase(db, id, "RIGHT_" + id, Trace.USER);
        this.grantee = grantee;
        this.grantedRole = grantedRole;
    }

    public Right(Database db, int id, RightOwner grantee, int grantedRight,
                 Table grantedRightOnTable) {
        initDbObjectBase(db, id, "" + id, Trace.USER);
        this.grantee = grantee;
        this.grantedRight = grantedRight;
        this.grantedTable = grantedRightOnTable;
    }

    private static boolean appendRight(StringBuilder buff, int right, int mask,
                                       String name, boolean comma) {
        if ((right & mask) != 0) {
            if (comma) {
                buff.append(", ");
            }
            buff.append(name);
            return true;
        }
        return comma;
    }

    public String getRights() {
        StringBuilder buff = new StringBuilder();
        if (grantedRight == ALL) {
            buff.append("ALL");
        } else {
            boolean comma = false;
            comma = appendRight(buff, grantedRight, SELECT, "SELECT", comma);
            comma = appendRight(buff, grantedRight, DELETE, "DELETE", comma);
            comma = appendRight(buff, grantedRight, INSERT, "INSERT", comma);
            comma = appendRight(buff, grantedRight, ALTER_ANY_SCHEMA,
                    "ALTER ANY SCHEMA", comma);
            appendRight(buff, grantedRight, UPDATE, "UPDATE", comma);
        }
        return buff.toString();
    }

    public Role getGrantedRole() {
        return grantedRole;
    }

    public Table getGrantedTable() {
        return grantedTable;
    }

    public DbObject getGrantee() {
        return grantee;
    }

    @Override
    public int getType() {
        return DbObject.RIGHT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        if (grantedTable != null) {
            grantee.revokeRight(grantedTable);
        } else {
            grantee.revokeRole(grantedRole);
        }
        grantedRole = null;
        grantedTable = null;
        grantee = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    public int getRightMask() {
        return grantedRight;
    }

    public void setRightMask(int rightMask) {
        grantedRight = rightMask;
    }

}
