/*
 * Copyright 2015 suning.com Holding Ltd.
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
// Created on 2015年4月18日
// $Id$

package com.suning.snfddal.tx;

import com.suning.snfddal.shards.DataSourceDispatcher;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class TransactionServiceImpl implements TransactionService, TransactionManager{

    /* (non-Javadoc)
     * @see com.suning.snfddal.tx.TransactionService#getTransactionManager()
     */
    @Override
    public TransactionManager getTransactionManager() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.tx.TransactionService#getManagedResource()
     */
    @Override
    public DataSourceDispatcher getManagedResource() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.tx.TransactionManager#begin()
     */
    @Override
    public void begin() throws TransactionException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.tx.TransactionManager#commit()
     */
    @Override
    public void commit() throws TransactionException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.tx.TransactionManager#rollback()
     */
    @Override
    public void rollback() throws TransactionException {
        // TODO Auto-generated method stub
        
    }

}
