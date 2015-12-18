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
package com.wplatform.ddal.route.algorithm;

import com.wplatform.ddal.route.rule.TableNode;
import com.wplatform.ddal.value.Value;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface Partitioner {

    /**
     * calling after the properties hash been set.
     *
     * @param tableNodes
     */
    void initialize(List<TableNode> tableNodes);

    /**
     * represent the sql condition: column=xx
     *
     * @param value
     * @return
     */
    Integer partition(Value value);

    /**
     * represent the sql condition: column >= xx and column <= xx
     *
     * @param beginValue
     * @param endValue
     * @return
     */
    Integer[] partition(Value beginValue, Value endValue);

    /**
     * represent the sql condition: column in (xx)
     *
     * @param values
     * @return
     */
    Integer[] partition(Value... values);
    
    

}
