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
// Created on 2015年4月12日
// $Id$

package com.suning.snfddal.excutor;

import java.util.concurrent.ExecutorService;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.Prepared;
import com.suning.snfddal.dispatch.RoutingHandler;
import com.suning.snfddal.dispatch.rule.GroupTableNode;
import com.suning.snfddal.dispatch.rule.RoutingResult;
import com.suning.snfddal.dispatch.rule.TableNode;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.result.ResultInterface;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class CommonPreparedExecutor<T extends Prepared> implements PreparedExecutor<T> {

    protected T prepared;
    protected Session session;
    protected ExecutorService executorService;
    protected RoutingHandler routingHandler;
    
    /**
     * 
     * @param session
     * @param prepared
     */
    public CommonPreparedExecutor(Session session,T prepared) {
        super();
        this.prepared = prepared;
        this.session = session;
    }


    @Override
    public ResultInterface executeQuery(int maxrows) {
        switch (prepared.getType()) {
        case CommandInterface.SELECT:
        case CommandInterface.CALL:
            RoutingResult rr = doRoute();
            TableNode[] tableNodes = rr.group();
            for (TableNode tableNode : tableNodes) {
                if(tableNode instanceof GroupTableNode) {
                    doTranslate((GroupTableNode)tableNode);
                } else {
                    doTranslate(tableNode);
                }
            }
            
            throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
        default:
            throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
        }

    }

    
    @Override
    public int executeUpdate() {
        switch (prepared.getType()) {
        case CommandInterface.SELECT:
        case CommandInterface.CALL:
            throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
        default:
            RoutingResult rr = doRoute();
            TableNode[] tableNodes = rr.group();
            for (TableNode tableNode : tableNodes) {
                if(tableNode instanceof GroupTableNode) {
                    doTranslate((GroupTableNode)tableNode);
                } else {
                    doTranslate(tableNode);
                }
            }
            return 0;
        }
    }
    
    
    protected T getPrepared() {
        return this.prepared;
    }
    
    
    protected abstract RoutingResult doRoute();
    protected abstract String doTranslate(TableNode tableNode);
    protected abstract String doTranslate(GroupTableNode tableNode);
    
    
    
    
    

}
