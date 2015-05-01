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

package com.suning.snfddal.excutor.support;

import com.suning.snfddal.command.ddl.CreateTable;
import com.suning.snfddal.dispatch.rule.GroupTableNode;
import com.suning.snfddal.dispatch.rule.RoutingResult;
import com.suning.snfddal.dispatch.rule.TableNode;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.excutor.CommonPreparedExecutor;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class CreateTableExecutor extends CommonPreparedExecutor<CreateTable> {

    private CreateTable createTable;
    /**
     * @param session
     * @param prepared
     */
    public CreateTableExecutor(Session session, CreateTable prepared) {
        super(session, prepared);
        createTable = prepared;
    }

    @Override
    protected RoutingResult doRoute() {
        
        
        return null;
    }


    @Override
    protected String doTranslate(TableNode tableNode) {
        
        CreateTable createTable = getPrepared();
        return null;
    }


    @Override
    protected String doTranslate(GroupTableNode tableNode) {
        return null;
    }
    
    
    public String getPlanSQL() {return null;}
    
    
    
}
