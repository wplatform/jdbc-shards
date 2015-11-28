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
package com.wplatform.ddal.excutor.dml;

import com.wplatform.ddal.command.dml.Select;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.result.ResultInterface;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class SelectExecutor extends CommonPreparedExecutor<Select> {

    /**
     * @param prepared
     */
    public SelectExecutor(Select prepared) {
        super(prepared);
    }

    @Override
    public ResultInterface executeQuery(int maxrows) {
        return super.executeQuery(maxrows);
    }
    

}
