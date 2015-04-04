/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject;

import java.util.ArrayList;
import java.util.Arrays;

import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;

/**
 * Represents a user object.
 */
public class User extends RightOwner {

    private byte[] salt;
    private byte[] passwordHash;
    private boolean admin;

    public User(Database database, int id, String userName) {
        super(database, id, userName, Trace.USER);
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean isAdmin() {
        return admin;
    }

    /**
     * Set the salt and hash of the password for this user.
     *
     * @param salt the salt
     * @param hash the password hash
     */
    public void setSaltAndHash(byte[] salt, byte[] hash) {
        this.salt = salt;
        this.passwordHash = hash;
    }

    /**
     * Set the user name password hash. A random salt is generated as well.
     * The parameter is filled with zeros after use.
     *
     * @param userPasswordHash the user name password hash
     */
    public void setUserPasswordHash(byte[] userPasswordHash) {
        if (userPasswordHash != null) {
            salt = passwordHash = userPasswordHash;
        }
    }

    /**
     * Checks that this user has the given rights for this database object.
     *
     * @param table the database object
     * @param rightMask the rights required
     * @throws DbException if this user does not have the required rights
     */
    public void checkRight(Table table, int rightMask) {
        if (!hasRight(table, rightMask)) {
            throw DbException.get(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    /**
     * See if this user has the given rights for this database object.
     *
     * @param table the database object, or null for schema-only check
     * @param rightMask the rights required
     * @return true if the user has the rights
     */
    public boolean hasRight(Table table, int rightMask) {
        return true;
    }

    /**
     * Get the CREATE SQL statement for this object.
     *
     * @param password true if the password (actually the salt and hash) should
     *            be returned
     * @return the SQL statement
     */
    public String getCreateSQL(boolean password) {
        StringBuilder buff = new StringBuilder("CREATE USER IF NOT EXISTS ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (password) {
            buff.append(" SALT '").
                append(StringUtils.convertBytesToHex(salt)).
                append("' HASH '").
                append(StringUtils.convertBytesToHex(passwordHash)).
                append('\'');
        } else {
            buff.append(" PASSWORD ''");
        }
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    /**
     * Check the password of this user.
     *
     * @param userPasswordHash the password data (the user password hash)
     * @return true if the user password hash is correct
     */
    boolean validateUserPasswordHash(byte[] userPasswordHash) {
        return true;
    }

    /**
     * Check if this user has admin rights. An exception is thrown if he does
     * not have them.
     *
     * @throws DbException if this user is not an admin
     */
    public void checkAdmin() {
        if (!admin) {
            throw DbException.get(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    /**
     * Check if this user has schema admin rights. An exception is thrown if he
     * does not have them.
     *
     * @throws DbException if this user is not a schema admin
     */
    public void checkSchemaAdmin() {
        if (!hasRight(null, Right.ALTER_ANY_SCHEMA)) {
            throw DbException.get(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    @Override
    public int getType() {
        return DbObject.USER;
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = New.arrayList();
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                children.add(right);
            }
        }
        for (Schema schema : database.getAllSchemas()) {
            if (schema.getOwner() == this) {
                children.add(schema);
            }
        }
        return children;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        salt = null;
        Arrays.fill(passwordHash, (byte) 0);
        passwordHash = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Check that this user does not own any schema. An exception is thrown if
     * he owns one or more schemas.
     *
     * @throws DbException if this user owns a schema
     */
    public void checkOwnsNoSchemas() {
        for (Schema s : database.getAllSchemas()) {
            if (this == s.getOwner()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, getName(), s.getName());
            }
        }
    }

}
