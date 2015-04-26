/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年4月15日
// $Id$

package com.suning.snfddal.shards;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class StandaloneSelector extends DataSourceSelector {

    private String shardName;
    private SmartDataSource standalone;

    private volatile boolean abnormal;

    public StandaloneSelector(String shardName, SmartDataSource standalone) {
        super();
        this.shardName = shardName;
        this.standalone = standalone;
    }

    @Override
    public SmartDataSource doSelect(Optional option) {
        if (abnormal) {
            return null;
        }
        return standalone;
    }

    @Override
    public SmartDataSource doSelect(Optional option, List<SmartDataSource> exclusive) {
        return doSelect(option);
    }

    @Override
    public String getShardName() {
        return shardName;
    }

    @Override
    public void doHandleAbnormal(SmartDataSource source) {
        if (!standalone.equals(source)) {
            throw new IllegalStateException("DataSource not matched." + standalone.toString()
                    + source.toString());
        }
        abnormal = true;

    }

    @Override
    public void doHandleWakeup(SmartDataSource source) {
        if (!standalone.equals(source)) {
            throw new IllegalStateException("DataSource not matched." + standalone.toString()
                    + source.toString());
        }
        abnormal = false;
    }

}
