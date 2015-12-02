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

package com.wplatform.ddal.dispatch.rule;


/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class GroupTableNode extends TableNode {

    private static final long serialVersionUID = 1L;

    private final String[] tableNames;

    private final String[] suffixes;

    public GroupTableNode(String shardName, String[] tableNames, String[] suffixes) {
        super(shardName, null, null);
        this.tableNames = tableNames;
        this.suffixes = suffixes;
    }

    @Override
    public String getObjectName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSuffix() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the tableNames
     */
    public String[] getTableNames() {
        return tableNames;
    }

    /**
     * @return the suffixes
     */
    public String[] getSuffixes() {
        return suffixes;
    }


}
