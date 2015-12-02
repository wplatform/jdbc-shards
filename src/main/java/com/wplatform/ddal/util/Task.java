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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A method call that is executed in a separate thread. If the method throws an
 * exception, it is wrapped in a RuntimeException.
 */
public abstract class Task implements Runnable {

    private static AtomicInteger counter = new AtomicInteger();

    /**
     * A flag indicating the get() method has been called.
     */
    protected volatile boolean stop;

    /**
     * The result, if any.
     */
    protected Object result;

    private volatile boolean finished;

    private Thread thread;

    private Exception ex;

    /**
     * The method to be implemented.
     *
     * @throws Exception any exception is wrapped in a RuntimeException
     */
    public abstract void call() throws Exception;

    @Override
    public void run() {
        try {
            call();
        } catch (Exception e) {
            this.ex = e;
        }
        finished = true;
    }

    /**
     * Start the thread.
     *
     * @return this
     */
    public Task execute() {
        return execute(getClass().getName() + ":" + counter.getAndIncrement());
    }

    /**
     * Start the thread.
     *
     * @param threadName the name of the thread
     * @return this
     */
    public Task execute(String threadName) {
        thread = new Thread(this, threadName);
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /**
     * Calling this method will set the stop flag and wait until the thread is
     * stopped.
     *
     * @return the result, or null
     * @throws RuntimeException if an exception in the method call occurs
     */
    public Object get() {
        Exception e = getException();
        if (e != null) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Whether the call method has returned (with or without exception).
     *
     * @return true if yes
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * Get the exception that was thrown in the call (if any).
     *
     * @return the exception or null
     */
    public Exception getException() {
        stop = true;
        if (thread == null) {
            throw new IllegalStateException("Thread not started");
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        if (ex != null) {
            return ex;
        }
        return null;
    }

}
