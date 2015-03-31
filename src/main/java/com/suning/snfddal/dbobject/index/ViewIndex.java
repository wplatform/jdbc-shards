/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import java.util.ArrayList;

import com.suning.snfddal.command.dml.Query;
import com.suning.snfddal.command.dml.SelectUnion;
import com.suning.snfddal.command.expression.Comparison;
import com.suning.snfddal.command.expression.Parameter;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.dbobject.table.TableView;
import com.suning.snfddal.engine.Constants;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.result.LocalResult;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.result.SortOrder;
import com.suning.snfddal.util.IntArray;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.SmallLRUCache;
import com.suning.snfddal.util.Utils;
import com.suning.snfddal.value.Value;

/**
 * This object represents a virtual index for a query.
 * Actually it only represents a prepared SELECT statement.
 */
public class ViewIndex extends BaseIndex {

    private final TableView view;
    private final String querySQL;
    private final ArrayList<Parameter> originalParameters;
    private final SmallLRUCache<IntArray, CostElement> costCache =
        SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private boolean recursive;
    private final int[] indexMasks;
    private Query query;
    private final Session createSession;

    public ViewIndex(TableView view, String querySQL,
            ArrayList<Parameter> originalParameters, boolean recursive) {
        initBaseIndex(view, 0, null, null, IndexType.createNonUnique(false));
        this.view = view;
        this.querySQL = querySQL;
        this.originalParameters = originalParameters;
        this.recursive = recursive;
        columns = new Column[0];
        this.createSession = null;
        this.indexMasks = null;
    }

    public ViewIndex(TableView view, ViewIndex index, Session session,
            int[] masks) {
        initBaseIndex(view, 0, null, null, IndexType.createNonUnique(false));
        this.view = view;
        this.querySQL = index.querySQL;
        this.originalParameters = index.originalParameters;
        this.recursive = index.recursive;
        this.indexMasks = masks;
        this.createSession = session;
        columns = new Column[0];
        if (!recursive) {
            query = getQuery(session, masks);
        }
    }

    public Session getSession() {
        return createSession;
    }

    @Override
    public String getPlanSQL() {
        return query == null ? null : query.getPlanSQL();
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("VIEW");
    }

    /**
     * A calculated cost value.
     */
    static class CostElement {

        /**
         * The time in milliseconds when this cost was calculated.
         */
        long evaluatedAt;

        /**
         * The cost.
         */
        double cost;
    }

    @Override
    public synchronized double getCost(Session session, int[] masks,
            TableFilter filter, SortOrder sortOrder) {
        if (recursive) {
            return 1000;
        }
        IntArray masksArray = new IntArray(masks == null ?
                Utils.EMPTY_INT_ARRAY : masks);
        CostElement cachedCost = costCache.get(masksArray);
        if (cachedCost != null) {
            long time = System.currentTimeMillis();
            if (time < cachedCost.evaluatedAt + Constants.VIEW_COST_CACHE_MAX_AGE) {
                return cachedCost.cost;
            }
        }
        Query q = (Query) session.prepare(querySQL, true);
        if (masks != null) {
            IntArray paramIndex = new IntArray();
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                paramIndex.add(i);
            }
            int len = paramIndex.size();
            for (int i = 0; i < len; i++) {
                int idx = paramIndex.get(i);
                int mask = masks[idx];
                int nextParamIndex = q.getParameters().size() + view.getParameterOffset();
                if ((mask & IndexCondition.EQUALITY) != 0) {
                    Parameter param = new Parameter(nextParamIndex);
                    q.addGlobalCondition(param, idx, Comparison.EQUAL_NULL_SAFE);
                } else {
                    if ((mask & IndexCondition.START) != 0) {
                        Parameter param = new Parameter(nextParamIndex);
                        q.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                    }
                    if ((mask & IndexCondition.END) != 0) {
                        Parameter param = new Parameter(nextParamIndex);
                        q.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                    }
                }
            }
            String sql = q.getPlanSQL();
            q = (Query) session.prepare(sql, true);
        }
        double cost = q.getCost();
        cachedCost = new CostElement();
        cachedCost.evaluatedAt = System.currentTimeMillis();
        cachedCost.cost = cost;
        costCache.put(masksArray, cachedCost);
        return cost;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, last, null);
    }

    private Cursor find(Session session, SearchRow first, SearchRow last,
            SearchRow intersection) {
        if (recursive) {
            LocalResult recResult = view.getRecursiveResult();
            if (recResult != null) {
                recResult.reset();
                return new ViewCursor(this, recResult, first, last);
            }
            if (query == null) {
                query = (Query) createSession.prepare(querySQL, true);
            }
            if (!(query instanceof SelectUnion)) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_2,
                        "recursive queries without UNION ALL");
            }
            SelectUnion union = (SelectUnion) query;
            if (union.getUnionType() != SelectUnion.UNION_ALL) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_2,
                        "recursive queries without UNION ALL");
            }
            Query left = union.getLeft();
            LocalResult r = left.query(0);
            LocalResult result = union.getEmptyResult();
            // ensure it is not written to disk,
            // because it is not closed normally
            result.setMaxMemoryRows(Integer.MAX_VALUE);
            while (r.next()) {
                result.addRow(r.currentRow());
            }
            Query right = union.getRight();
            r.reset();
            view.setRecursiveResult(r);
            while (true) {
                r = right.query(0);
                if (r.getRowCount() == 0) {
                    break;
                }
                while (r.next()) {
                    result.addRow(r.currentRow());
                }
                r.reset();
                view.setRecursiveResult(r);
            }
            view.setRecursiveResult(null);
            result.done();
            return new ViewCursor(this, result, first, last);
        }
        ArrayList<Parameter> paramList = query.getParameters();
        if (originalParameters != null) {
            for (int i = 0, size = originalParameters.size(); i < size; i++) {
                Parameter orig = originalParameters.get(i);
                int idx = orig.getIndex();
                Value value = orig.getValue(session);
                setParameter(paramList, idx, value);
            }
        }
        int len;
        if (first != null) {
            len = first.getColumnCount();
        } else if (last != null) {
            len = last.getColumnCount();
        } else if (intersection != null) {
            len = intersection.getColumnCount();
        } else {
            len = 0;
        }
        int idx = originalParameters == null ? 0 : originalParameters.size();
        idx += view.getParameterOffset();
        for (int i = 0; i < len; i++) {
            int mask = indexMasks[i];
            if ((mask & IndexCondition.EQUALITY) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexCondition.START) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexCondition.END) != 0) {
                setParameter(paramList, idx++, last.getValue(i));
            }
        }
        LocalResult result = query.query(0);
        return new ViewCursor(this, result, first, last);
    }

    private static void setParameter(ArrayList<Parameter> paramList, int x,
            Value v) {
        if (x >= paramList.size()) {
            // the parameter may be optimized away as in
            // select * from (select null as x) where x=1;
            return;
        }
        Parameter param = paramList.get(x);
        param.setValue(v);
    }

    private Query getQuery(Session session, int[] masks) {
        Query q = (Query) session.prepare(querySQL, true);
        if (masks == null) {
            return q;
        }
        if (!q.allowGlobalConditions()) {
            return q;
        }
        int firstIndexParam = originalParameters == null ?
                0 : originalParameters.size();
        firstIndexParam += view.getParameterOffset();
        IntArray paramIndex = new IntArray();
        int indexColumnCount = 0;
        for (int i = 0; i < masks.length; i++) {
            int mask = masks[i];
            if (mask == 0) {
                continue;
            }
            indexColumnCount++;
            paramIndex.add(i);
            if (Integer.bitCount(mask) > 1) {
                // two parameters for range queries: >= x AND <= y
                paramIndex.add(i);
            }
        }
        int len = paramIndex.size();
        ArrayList<Column> columnList = New.arrayList();
        for (int i = 0; i < len;) {
            int idx = paramIndex.get(i);
            columnList.add(table.getColumn(idx));
            int mask = masks[idx];
            if ((mask & IndexCondition.EQUALITY) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.EQUAL_NULL_SAFE);
                i++;
            }
            if ((mask & IndexCondition.START) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                i++;
            }
            if ((mask & IndexCondition.END) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                i++;
            }
        }
        columns = new Column[columnList.size()];
        columnList.toArray(columns);

        // reconstruct the index columns from the masks
        this.indexColumns = new IndexColumn[indexColumnCount];
        this.columnIds = new int[indexColumnCount];
        for (int type = 0, indexColumnId = 0; type < 2; type++) {
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                if (type == 0) {
                    if ((mask & IndexCondition.EQUALITY) == 0) {
                        // the first columns need to be equality conditions
                        continue;
                    }
                } else {
                    if ((mask & IndexCondition.EQUALITY) != 0) {
                        // after that only range conditions
                        continue;
                    }
                }
                IndexColumn c = new IndexColumn();
                c.column = table.getColumn(i);
                indexColumns[indexColumnId] = c;
                columnIds[indexColumnId] = c.column.getColumnId();
                indexColumnId++;
            }
        }

        String sql = q.getPlanSQL();
        q = (Query) session.prepare(sql, true);
        return q;
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("VIEW");
    }

    public void setRecursive(boolean value) {
        this.recursive = value;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    public boolean isRecursive() {
        return recursive;
    }

}
