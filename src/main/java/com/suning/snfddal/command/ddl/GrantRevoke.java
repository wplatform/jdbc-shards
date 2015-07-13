/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.dbobject.RightOwner;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.util.New;

import java.util.ArrayList;

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
