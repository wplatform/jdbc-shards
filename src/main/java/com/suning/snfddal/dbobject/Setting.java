/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject;

import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.Trace;

/**
 * A persistent database setting.
 */
public class Setting extends DbObjectBase {

    private int intValue;
    private String stringValue;

    public Setting(Database database, int id, String settingName) {
        initDbObjectBase(database, id, settingName, Trace.SETTING);
    }

    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int value) {
        intValue = value;
    }

    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String value) {
        stringValue = value;
    }

    @Override
    public int getType() {
        return DbObject.SETTING;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("RENAME");
    }

}
