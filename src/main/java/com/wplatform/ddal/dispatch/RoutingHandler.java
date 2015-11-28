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

package com.wplatform.ddal.dispatch;

import java.util.List;

import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.function.PartitionFunction;
import com.wplatform.ddal.dispatch.rule.RoutingResult;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.SearchRow;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface RoutingHandler {
    
    PartitionFunction getPartitionFunction(TableMate table);
    
    RoutingResult doRoute(TableMate table, SearchRow row);

    RoutingResult doRoute(TableMate table, SearchRow stard, SearchRow end);

    RoutingResult doRoute(TableMate table, Session session, List<IndexCondition> indexConditions);

}
