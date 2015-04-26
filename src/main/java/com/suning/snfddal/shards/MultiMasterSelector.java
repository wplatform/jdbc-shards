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
import java.util.Set;

import com.suning.snfddal.util.New;


/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MultiMasterSelector extends DataSourceSelector {
    private String shardName;
    private List<SmartDataSource> registered;
    private Set<SmartDataSource> readable = New.copyOnWriteArraySet();
    private Set<SmartDataSource> writable = New.copyOnWriteArraySet();
    private volatile LoadBalance writableLoadBalance;
    private volatile LoadBalance readableLoadBalance;
    
    /**
     * @param shardName
     * @param writable
     * @param readable
     */
    public MultiMasterSelector(String shardName, List<SmartDataSource> registered) {
        List<SmartDataSource> writable = New.arrayList();
        List<SmartDataSource> readable = New.arrayList();
        for (SmartDataSource item : registered) {
            if(!item.isReadOnly() && item.getwWeight() > 0) {
                writable.add(item);
            }
            if(item.getrWeight() > 0) {
                readable.add(item);
            }
        }
        if(writable.size() < 1) {
            throw new IllegalStateException();
        }
        if(readable.size() < 1) {
            throw new IllegalStateException();
        }
        this.registered = registered;
        this.writable.addAll(writable);
        this.readable.addAll(readable);
        this.writableLoadBalance = new LoadBalance(writable, false);
        this.readableLoadBalance = new LoadBalance(readable, true);
    }


    @Override
    public SmartDataSource doSelect(Optional option) {
        if (!option.readOnly) {
            return writableLoadBalance.load();
        } else {
            return readableLoadBalance.load();
        }

    }

    @Override
    public SmartDataSource doSelect(Optional option, List<SmartDataSource> exclusive) {
        for (SmartDataSource marker : registered) {
            if(exclusive.contains(marker)) {
                continue;
            }
            if (!option.readOnly && marker.isReadOnly()) {
                continue;
            }
            return marker;
        }
        return null;
    }

    @Override
    public String getShardName() {
        return shardName;
    }

    @Override
    public void doHandleAbnormal(SmartDataSource source) {
        if (!registered.contains(source)) {
            throw new IllegalStateException(shardName + "datasource not matched. " + source);
        }
        if(!source.isReadOnly() && writable.remove(source)) {
            this.writableLoadBalance = new LoadBalance(writable, false);
        }
        if(readable.remove(source)) {
            readableLoadBalance = new LoadBalance(readable, true);
        }
        
        
    }

    @Override
    public void doHandleWakeup(SmartDataSource source) {
        if (!registered.contains(source)) {
            throw new IllegalStateException(shardName + " datasource not matched. " + source);
        }
        if(!source.isReadOnly() && source.getwWeight() > 0 && writable.add(source)) {
            this.writableLoadBalance = new LoadBalance(writable, true);
        }
        if(source.getrWeight() > 0 && readable.add(source)) {
            this.readableLoadBalance = new LoadBalance(readable, true);
        }
        
    }
    
}
