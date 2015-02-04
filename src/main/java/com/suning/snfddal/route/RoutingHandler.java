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
// Created on 2015年2月3日
// $Id$

package com.suning.snfddal.route;

import java.util.List;

import com.suning.snfddal.dbobject.index.IndexCondition;
import com.suning.snfddal.dbobject.table.MappedTable;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.route.rule.RoutingResult;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface RoutingHandler {
    
    RoutingResult doRoute(MappedTable table, SearchRow row);
    
    RoutingResult doRoute(MappedTable table, SearchRow stard, SearchRow end);
    
    RoutingResult doRoute(MappedTable table, Session session, List<IndexCondition> indexConditions);
    
}
