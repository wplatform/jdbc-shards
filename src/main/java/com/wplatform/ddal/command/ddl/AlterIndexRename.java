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
package com.wplatform.ddal.command.ddl;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

/**
 * This class represents the statement
 * ALTER INDEX RENAME
 */
public class AlterIndexRename extends DefineCommand {

    private Index oldIndex;
    private String newIndexName;

    public AlterIndexRename(Session session) {
        super(session);
    }

    public void setOldIndex(Index index) {
        oldIndex = index;
    }

    public void setNewName(String name) {
        newIndexName = name;
    }


    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_INDEX_RENAME;
    }

    public Index getOldIndex() {
        return oldIndex;
    }

    public String getNewIndexName() {
        return newIndexName;
    }

    
    

}
