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

import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.command.dml.Merge;
import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.Parameter;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MergeExecutor extends PreparedRoutingExecutor<Merge> {

    /**
     * @param prepared
     */
    public MergeExecutor(Merge prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        Column[] columns = prepared.getColumns();
        TableMate table = castTableMate(prepared.getTable());
        ArrayList<Expression[]> list = prepared.getList();
        table.check();

        int count;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        prepared.setCurrentRowNumber(0);
        if (list.size() > 0) {
            count = 0;
            for (int x = 0, size = list.size(); x < size; x++) {
                prepared.setCurrentRowNumber(x + 1);
                Expression[] expr = list.get(x);
                Row newRow = table.getTemplateRow();
                for (int i = 0, len = columns.length; i < len; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        try {
                            Value v = c.convert(e.getValue(session));
                            newRow.setValue(index, v);
                        } catch (DbException ex) {
                            throw prepared.setRow(ex, count, Prepared.getSQL(expr));
                        }
                    }
                }
                merge(newRow);
                count++;
            }
        } else {
            Query query = prepared.getQuery();
            ResultInterface rows = query.query(0);
            count = 0;
            while (rows.next()) {
                count++;
                Value[] r = rows.currentRow();
                Row newRow = table.getTemplateRow();
                prepared.setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    int index = c.getColumnId();
                    try {
                        Value v = c.convert(r[j]);
                        newRow.setValue(index, v);
                    } catch (DbException ex) {
                        throw prepared.setRow(ex, count, Prepared.getSQL(r));
                    }
                }
                merge(newRow);
            }
            rows.close();
        }
        return count;
    }

    private void merge(Row row) {
        TableMate table = castTableMate(prepared.getTable());
        Prepared update = prepared.getUpdate();
        Column[] columns = prepared.getColumns();
        Column[] keys = prepared.getKeys();

        ArrayList<Parameter> k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            Parameter p = k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
            }
            Parameter p = k.get(columns.length + i);
            p.setValue(v);
        }
        int count = update.update();
        if (count == 0) {
            try {
                table.validateConvertUpdateSequence(session, row);
                updateRow(table, row);
            } catch (DbException e) {
                throw e;
            }
        } else if (count != 1) {
            throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getSQL());
        }
    }

    @Override
    protected List<Value> doTranslate(TableNode node, SearchRow row, StatementBuilder buff) {
        String forTable = node.getCompositeObjectName();
        TableMate table = castTableMate(prepared.getTable());
        Column[] columns = table.getColumns();
        return buildInsert(forTable, columns, row, buff);

    }



}
