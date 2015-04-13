package com.suning.snfddal.excutor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class NamedPoolExecutor extends ThreadPoolExecutor {

    private static final String SQLexecution_thread_prefix = "sql-execution";

    private static final int CORE_NUMBER = Runtime.getRuntime().availableProcessors();

    public NamedPoolExecutor(String name) {
        super(CORE_NUMBER, CORE_NUMBER, 120, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(SQLexecution_thread_prefix, true));
        allowCoreThreadTimeOut(true);

    }

    public static class NamedThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);

        private final AtomicInteger threadNumber = new AtomicInteger(1);

        private final String prefix;

        private final boolean daemon;

        public NamedThreadFactory() {
            this("pool-" + poolNumber.getAndIncrement(), false);
        }

        public NamedThreadFactory(String prefix) {
            this(prefix, false);
        }

        public NamedThreadFactory(String prefix, boolean daemon) {
            this.prefix = prefix + "-thread-";
            this.daemon = daemon;
        }

        public Thread newThread(Runnable runnable) {
            String name = prefix + threadNumber.getAndIncrement();
            Thread ret = new Thread(runnable, name);
            ret.setDaemon(daemon);
            return ret;
        }
    }
}
