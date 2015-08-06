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
package com.wplatform.ddal.engine;

import com.wplatform.ddal.command.Prepared;

/**
 * Represents a procedure. Procedures are implemented for PostgreSQL
 * compatibility.
 */
public class Procedure {

    private final String name;
    private final Prepared prepared;

    public Procedure(String name, Prepared prepared) {
        this.name = name;
        this.prepared = prepared;
    }

    public String getName() {
        return name;
    }

    public Prepared getPrepared() {
        return prepared;
    }

}
