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
// Created on 2015年1月28日
// $Id$

package com.suning.snfddal.route;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MultiNodeExecutor {
    
    
    private final static ThreadPoolExecutor executorService = newThreadPoolExecutor();


    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return executorService;
    }

    public static <T> List<T> execute(List<Callable<T>> calls) {
        List<T> results = New.arrayList(calls.size());
        int size = calls.size();
        List<Future<T>> futures = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            futures.add(executorService.submit(calls.get(i)));
        }
        for (Future<T> future : futures) {
            try {
                T result = future.get();
                results.add(result);
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            } catch (ExecutionException e) {
                Throwable ex = e.getCause();
                throw DbException.convert(ex);
            } catch (Exception e) {
                throw DbException.convert(e);
            }   
        }
        return results;
    }
    

    /**
     * Create a cached ThreadPoolExecutor.
     *
     * @param name
     * @param size
     * @param maxSize
     * @param isDaemon
     * @return
     */
    public static ThreadPoolExecutor newThreadPoolExecutor() {
        NamedThreadFactory factory = new NamedThreadFactory("MultiNodeExecutor", false);
        return new ThreadPoolExecutor(0, 100, 30, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                factory, new ThreadPoolExecutor.AbortPolicy());
    }
    
    

    private static class NamedThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final String namePrefix;
        private final AtomicInteger threadId;
        private final boolean isDaemon;

        public NamedThreadFactory(String name, boolean isDaemon) {
            SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = name;
            this.threadId = new AtomicInteger(0);
            this.isDaemon = isDaemon;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadId.getAndIncrement());
            t.setDaemon(isDaemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
