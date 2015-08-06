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

import com.wplatform.ddal.dbobject.RightOwner;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;

/**
 * This class represents the statements
 * GRANT RIGHT,
 * GRANT ROLE,
 * REVOKE RIGHT,
 * REVOKE ROLE
 */
public class GrantRevoke extends DefineCommand {

    private final ArrayList<Table> tables = New.arrayList();
    private ArrayList<String> roleNames;
    private int operationType;
    private int rightMask;
    private RightOwner grantee;

    public GrantRevoke(Session session) {
        super(session);
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    /**
     * Add the specified right bit to the rights bitmap.
     *
     * @param right the right bit
     */
    public void addRight(int right) {
        this.rightMask |= right;
    }

    /**
     * Add the specified role to the list of roles.
     *
     * @param roleName the role
     */
    public void addRoleName(String roleName) {
        if (roleNames == null) {
            roleNames = New.arrayList();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) {
        Database db = session.getDatabase();
        grantee = db.findUser(granteeName);
        if (grantee == null) {
            grantee = db.findRole(granteeName);
            if (grantee == null) {
                throw DbException.get(ErrorCode.USER_OR_ROLE_NOT_FOUND_1, granteeName);
            }
        }
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    /**
     * Add the specified table to the list of tables.
     *
     * @param table the table
     */
    public void addTable(Table table) {
        tables.add(table);
    }

    @Override
    public int getType() {
        return operationType;
    }

    /**
     * @return true if this command is using Roles
     */
    public boolean isRoleMode() {
        return roleNames != null;
    }

    /**
     * @return true if this command is using Rights
     */
    public boolean isRightMode() {
        return rightMask != 0;
    }
}
