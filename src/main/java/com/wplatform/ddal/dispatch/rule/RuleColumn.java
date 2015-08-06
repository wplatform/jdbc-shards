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
// Created on 2014年4月19日
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RuleColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Map<String, String> typeMap = new HashMap<String, String>();

    static {
        typeMap.put("int", "java.lang.Integer");
        typeMap.put("short", "java.lang.Short");
        typeMap.put("long", "java.lang.Long");
        typeMap.put("byte", "java.lang.Byte");
        typeMap.put("char", "java.lang.Character");
        typeMap.put("boolean", "java.lang.Boolean");
        typeMap.put("float", "java.lang.Float");
        typeMap.put("double", "java.lang.Double");
        typeMap.put("string", "java.lang.String");
        typeMap.put("date", "java.sql.Date");
        typeMap.put("time", "java.sql.Time");
        typeMap.put("timestamp", "java.sql.Timestamp");
        typeMap.put("bigdecimal", "java.math.BigDecimal");
    }

    private String name;

    private String type;

    private boolean required = true;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = typeMap.containsKey(type) ? typeMap.get(type) : type;
    }

    /**
     * @return the required
     */
    public boolean isRequired() {
        return required;
    }

    /**
     * @param required the required to set
     */
    public void setRequired(boolean required) {
        this.required = required;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.toLowerCase().hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RuleColumn other = (RuleColumn) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equalsIgnoreCase(other.name))
            return false;
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "${" + name + "}";
    }


}
