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
package com.wplatform.ddal.message;

/**
 * The backend of the trace system must implement this interface. Two
 * implementations are supported: the (default) native trace writer
 * implementation that can write to a file and to system out, and an adapter
 * that uses SLF4J (Simple Logging Facade for Java).
 */
interface TraceWriter {

    /**
     * Set the name of the database or trace object.
     *
     * @param name the new name
     */
    void setName(String name);

    /**
     * Write a message.
     *
     * @param level  the trace level
     * @param module the name of the module
     * @param s      the message
     * @param t      the exception (may be null)
     */
    void write(int level, String module, String s, Throwable t);

    /**
     * Check the given trace / log level is enabled.
     *
     * @param level the level
     * @return true if the level is enabled
     */
    boolean isEnabled(int level);

}
