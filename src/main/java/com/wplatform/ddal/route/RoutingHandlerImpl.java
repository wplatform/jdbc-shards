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
// Created on 2015年2月3日
// $Id$

package com.wplatform.ddal.route;

import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.route.rule.*;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingHandlerImpl implements RoutingHandler {

    private RoutingCalculator trc;

    public RoutingHandlerImpl(Database database) {
        this.trc = new RoutingCalculatorImpl();
    }

    @Override
    public RoutingResult doRoute(TableMate table, SearchRow row) {
        TableRouter tr = table.getTableRouter();
        if (tr != null)
            try {
                return getRoutingResult(table, row);
            } catch (TableRoutingException e) {
                throw e;
            } catch (Exception e) {
                throw new TableRoutingException(table.getName() + " routing error.");
            }
        else {
            return fixedRoutingResult(table.getShards());
        }

    }

    private RoutingResult getRoutingResult(TableMate table, SearchRow row) {
        TableRouter tr = table.getTableRouter();
        Column[] ruleCols = table.getRuleColumns();
        List<RoutingArgument> args = New.arrayList(ruleCols.length);
        for (Column ruleCol : ruleCols) {
            Value v = row.getValue(ruleCol.getColumnId());
            List<Value> value = New.arrayList(1);
            value.add(v);
            RoutingArgument arg = new RoutingArgument(value);
            args.add(arg);
        }
        RoutingResult rr;
        if (args.size() == 1) {
            RoutingArgument argument = args.get(0);
            rr = trc.calculate(tr, argument);
        } else {
            rr = trc.calculate(tr, args);
        }
        if (rr.isMultipleNode()) {
            throw new TableRoutingException(table.getName() + " routing error.");
        }
        return rr;
    }


    @Override
    public RoutingResult doRoute(Session session, TableMate table, List<IndexCondition> idxConds) {
        TableRouter tr = table.getTableRouter();
        if (tr != null)
            try {
                RoutingAnalyzer analysor = new RoutingAnalyzer(table, idxConds);
                if (analysor.isAlwaysFalse()) {
                    return RoutingResult.emptyResult();
                }
                Column[] ruleCols = table.getRuleColumns();
                List<RoutingArgument> args = New.arrayList(ruleCols.length);
                for (Column ruleCol : ruleCols) {
                    RoutingArgument arg = analysor.doAnalyse(session, ruleCol);
                    args.add(arg);
                }
                RoutingResult rr;
                if (args.size() == 1) {
                    RoutingArgument argument = args.get(0);
                    rr = trc.calculate(tr, argument);
                } else {
                    rr = trc.calculate(tr, args);
                }
                return rr;
            } catch (TableRoutingException e) {
                throw e;
            } catch (Exception e) {
                throw new TableRoutingException(table.getName() + " routing error.");
            }
        else {
            return fixedRoutingResult(table.getShards());
        }

    }

    private RoutingResult fixedRoutingResult(TableNode... tableNode) {
        RoutingResult result = RoutingResult.fixedResult(tableNode);
        return result;
    }

}
