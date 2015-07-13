/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.table;

import com.suning.snfddal.dbobject.index.Index;

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {

    /**
     * The cost.
     */
    double cost;

    private Index index;
    private PlanItem joinPlan;
    private PlanItem nestedJoinPlan;

    public Index getIndex() {
        return index;
    }

    void setIndex(Index index) {
        this.index = index;
    }

    PlanItem getJoinPlan() {
        return joinPlan;
    }

    void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

    PlanItem getNestedJoinPlan() {
        return nestedJoinPlan;
    }

    void setNestedJoinPlan(PlanItem nestedJoinPlan) {
        this.nestedJoinPlan = nestedJoinPlan;
    }

}
