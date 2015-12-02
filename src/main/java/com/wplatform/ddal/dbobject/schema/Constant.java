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
package com.wplatform.ddal.dbobject.schema;

import com.wplatform.ddal.command.expression.ValueExpression;
import com.wplatform.ddal.dbobject.DbObject;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 */
public class Constant extends SchemaObjectBase {

    private Value value;
    private ValueExpression expression;

    public Constant(Schema schema, int id, String name) {
        initSchemaObjectBase(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public int getType() {
        return DbObject.CONSTANT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

    public ValueExpression getValue() {
        return expression;
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }
    
    @Override
    public String toString() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }
    

}
