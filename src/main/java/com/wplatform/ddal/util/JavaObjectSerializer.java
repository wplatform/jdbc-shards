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
package com.wplatform.ddal.util;

/**
 * Custom serialization mechanism for java objects being stored in column of
 * type OTHER.
 *
 * @author Sergi Vladykin
 */
public interface JavaObjectSerializer {

    /**
     * Serialize object to byte array.
     *
     * @param obj the object to serialize
     * @return the byte array of the serialized object
     */
    byte[] serialize(Object obj) throws Exception;

    /**
     * Deserialize object from byte array.
     *
     * @param bytes the byte array of the serialized object
     * @return the object
     */
    Object deserialize(byte[] bytes) throws Exception;

}
