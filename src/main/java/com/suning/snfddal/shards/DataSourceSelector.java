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
// Created on 2015年4月13日
// $Id$

package com.suning.snfddal.shards;

import com.suning.snfddal.util.New;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class DataSourceSelector implements Failover {

    public static DataSourceSelector create(String shardName, List<SmartDataSource> items) {
        DataSourceSelector selector;
        if (items.size() == 1) {
            SmartDataSource ds = items.get(0);
            if (ds.isReadOnly()) {
                throw new IllegalArgumentException();
            }
            selector = new StandaloneSelector(shardName, ds);
        }
        List<DataSourceMarker> writableDb = New.arrayList(items.size());
        List<DataSourceMarker> readableDb = New.arrayList(items.size());
        for (DataSourceMarker item : items) {
            if (!item.isReadOnly()) {
                writableDb.add(item);
            }
            readableDb.add(item);
        }
        if (writableDb.isEmpty() || readableDb.isEmpty()) {
            throw new IllegalArgumentException("No writable datasource.");
        }
        if (writableDb.size() == 1) {
            selector = new OneMasterSelector(shardName, items);
        } else {
            selector = new MultiMasterSelector(shardName, items);
        }
        return selector;
    }

    public abstract String getShardName();

    public abstract SmartDataSource doSelect(Optional option);

    public abstract SmartDataSource doSelect(Optional option, List<SmartDataSource> exclusive);

}
