/*
 * Copyright 2014 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2014年6月21日
// $Id$

package com.suning.snfddal.tx;

import java.sql.SQLException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TransactionException extends SQLException {

    private static final long serialVersionUID = 1L;

    /**
     *
     */
    public TransactionException() {
        super();
    }

    /**
     * @param reason
     * @param sqlState
     * @param vendorCode
     * @param cause
     */
    public TransactionException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }

    /**
     * @param reason
     * @param SQLState
     * @param vendorCode
     */
    public TransactionException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    /**
     * @param reason
     * @param sqlState
     * @param cause
     */
    public TransactionException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    /**
     * @param reason
     * @param SQLState
     */
    public TransactionException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    /**
     * @param reason
     * @param cause
     */
    public TransactionException(String reason, Throwable cause) {
        super(reason, cause);
    }

    /**
     * @param reason
     */
    public TransactionException(String reason) {
        super(reason);
    }

    /**
     * @param cause
     */
    public TransactionException(Throwable cause) {
        super(cause);
    }


}
