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

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ValueExpression;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.value.ValueInt;

/**
 * This class represents the statement
 * SET
 */
public class Set extends Prepared {

    private final int type;
    private Expression expression;
    private String stringValue;
    private String[] stringValueList;

    public Set(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    @Override
    public boolean isTransactional() {
        switch (type) {
            case SetTypes.CLUSTER:
            case SetTypes.VARIABLE:
            case SetTypes.QUERY_TIMEOUT:
            case SetTypes.LOCK_TIMEOUT:
            case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            case SetTypes.TRACE_LEVEL_FILE:
            case SetTypes.THROTTLE:
            case SetTypes.SCHEMA:
            case SetTypes.SCHEMA_SEARCH_PATH:
            case SetTypes.RETENTION_TIME:
                return true;
            default:
        }
        return false;
    }


    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }
    
    //getters
    
    public int getSetType() {
        return type;
    }
    
    public Expression getExpression() {
        return expression;
    }

    public String getStringValue() {
        return stringValue;
    }

    public String[] getStringValueList() {
        return stringValueList;
    }

    
}
