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
// Created on 2014年4月24日
// $Id$

package com.wplatform.ddal.dispatch.rule;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RuleEvaluateException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     *
     */
    public RuleEvaluateException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public RuleEvaluateException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public RuleEvaluateException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public RuleEvaluateException(Throwable cause) {
        super(cause);
    }


}
