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
package com.wplatform.ddal.excutor.dml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wplatform.ddal.command.dml.Update;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class UpdateExecutor extends PreparedRoutingExecutor<Update> {

    /**
     * @param prepared
     */
    public UpdateExecutor(Update prepared) {
        super(prepared);
    }
    
    
    @Override
    public int executeUpdate() {
        TableFilter tableFilter = prepared.getTableFilter();
        TableMate table = castTableMate(tableFilter.getTable());
        List<Column> columns = prepared.getColumns();
        Map<Column, Expression> valueMap = prepared.getExpressionMap();
        table.check();
        session.getUser().checkRight(table, Right.UPDATE);
        
        Row updateRow = table.getTemplateRow();
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = valueMap.get(c);
            int index = c.getColumnId();
            if (e != null) {
                // e can be null (DEFAULT)
                e = e.optimize(session);
                try {
                    Value v = c.convert(e.getValue(session));
                    updateRow.setValue(index, v);
                } catch (DbException ex) {
                    ex.addSQL("evaluate expression " + e.getSQL());
                    throw ex;
                }
            }
        }
        return updateRow(table, updateRow, tableFilter.getIndexConditions());
        
    }


    @Override
    protected List<Value> doTranslate(TableNode node, SearchRow row, StatementBuilder buff) {
        ArrayList<Value> params = New.arrayList();
        TableFilter tableFilter = prepared.getTableFilter();
        String forTable = node.getCompositeObjectName();
        List<Column> columns = prepared.getColumns();
        Expression condition = prepared.getCondition();
        Expression limitExpr = prepared.getLimitExpr();

        buff.append("UPDATE ");
        buff.append(identifier(forTable)).append(" SET ");
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL()).append(" = ");
            
            Value v = row.getValue(i);
            buff.appendExceptFirst(", ");
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
            }
        }
        if (condition != null) {
            condition.exportParameters(tableFilter, params);
            buff.append(" WHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            limitExpr.exportParameters(tableFilter, params);
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
        }
        return params;
    }
    
    

}
