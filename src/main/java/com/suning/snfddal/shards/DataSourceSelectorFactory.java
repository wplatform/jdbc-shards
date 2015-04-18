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

import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class DataSourceSelectorFactory {

    public DataSourceSelector createDataSourceSelector(String shardName, List<DataSourceMarker> items) {
        DataSourceSelector selector;
        if(items.size() == 1) {
            DataSourceMarker ds = items.get(0);
            if(!ds.isWritable()) {
                throw new IllegalArgumentException();
            }
            selector =  new StandaloneSelector(shardName, ds);
        }
        List<DataSourceMarker> writable = New.arrayList(items.size());
        List<DataSourceMarker> readable = New.arrayList(items.size());
        for (DataSourceMarker item : items) {
            if(item.isWritable()) {
                writable.add(item);
            } else {
                readable.add(item);
            }
        }
        if(writable.isEmpty() || readable.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if(writable.size() == 1) {
            selector =  new OneMasterSelector(shardName, writable.get(0),readable);
        } else {
            selector =  new MultiMasterSelector(shardName, writable,readable);
        }
        return selector;
    }
    
}
