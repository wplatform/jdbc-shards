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
// Created on 2015年4月16日
// $Id$

package com.suning.snfddal.shards;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface DataSourceDispatcher {

    public Connection doDispatch(Optional optional) throws SQLException;
        
}
