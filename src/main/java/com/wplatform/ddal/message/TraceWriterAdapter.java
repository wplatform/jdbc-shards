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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This adapter sends log output to SLF4J. SLF4J supports multiple
 * implementations such as Logback, Log4j, Jakarta Commons Logging (JCL), JDK
 * 1.4 logging, x4juli, and Simple Log. To use SLF4J, you need to add the
 * required jar files to the classpath, and set the trace level to 4 when
 * opening a database:
 * <p>
 * <pre>
 * jdbc:h2:&tilde;/test;TRACE_LEVEL_FILE=4
 * </pre>
 * <p>
 * The logger name is 'jdbc-shards'.
 */
public class TraceWriterAdapter implements TraceWriter {

    private final Logger logger = LoggerFactory.getLogger("jdbc-shards");
    private String name;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isEnabled(int level) {
        switch (level) {
            case TraceSystem.DEBUG:
                return logger.isDebugEnabled();
            case TraceSystem.INFO:
                return logger.isInfoEnabled();
            case TraceSystem.ERROR:
                return logger.isErrorEnabled();
            default:
                return false;
        }
    }

    @Override
    public void write(int level, String module, String s, Throwable t) {
        if (isEnabled(level)) {
            if (name != null) {
                s = name + ":" + module + " " + s;
            } else {
                s = module + " " + s;
            }
            switch (level) {
                case TraceSystem.DEBUG:
                    logger.debug(s, t);
                    break;
                case TraceSystem.INFO:
                    logger.info(s, t);
                    break;
                case TraceSystem.ERROR:
                    logger.error(s, t);
                    break;
                default:
            }
        }
    }

}
