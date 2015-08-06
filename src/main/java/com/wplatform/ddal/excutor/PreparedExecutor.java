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
// Created on 2015年4月10日
// $Id$

package com.wplatform.ddal.excutor;

import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.ResultInterface;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface PreparedExecutor<T extends Prepared> {
    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    ResultInterface executeQuery(int maxrows);

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    int executeUpdate();
}
