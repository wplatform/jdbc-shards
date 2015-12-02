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
// Created on 2015年4月12日
// $Id$

package com.wplatform.ddal.excutor.ddl;

import com.wplatform.ddal.command.ddl.DropIndex;
import com.wplatform.ddal.dispatch.rule.TableNode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateIndexExecutor extends DefineCommandExecutor<DropIndex> {

    /**
     * @param session
     * @param prepared
     */
    public CreateIndexExecutor(DropIndex prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        return 0;
    }

    @Override
    protected String doTranslate(TableNode tableNode) {
        return null;
    }

}
