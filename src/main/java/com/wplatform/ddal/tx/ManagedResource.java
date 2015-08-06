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
// Created on 2015年4月18日
// $Id$

package com.wplatform.ddal.tx;

import java.sql.SQLException;

import com.wplatform.ddal.shards.DataSourceDispatcher;
import com.wplatform.ddal.shards.DataSourceMarker;
import com.wplatform.ddal.shards.Optional;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ManagedResource implements DataSourceDispatcher {

    private DataSourceDispatcher delegate;

    /**
     * @param delegate
     */
    public ManagedResource(DataSourceDispatcher delegate) {
        this.delegate = delegate;
    }

    @Override
    public DataSourceMarker doDispatch(Optional optional) throws SQLException {
        // TODO Auto-generated method stub
        return delegate.doDispatch(optional);
    }

}
