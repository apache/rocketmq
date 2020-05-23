/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.logging.inner;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

public class LoggingEvent implements java.io.Serializable {

    transient public final String fqnOfCategoryClass;

    transient private Object message;

    transient private Level level;

    transient private Logger logger;

    private String renderedMessage;

    private String threadName;

    public final long timeStamp;

    private Throwable throwable;

    public LoggingEvent(String fqnOfCategoryClass, Logger logger,
                        Level level, Object message, Throwable throwable) {
        this.fqnOfCategoryClass = fqnOfCategoryClass;
        this.message = message;
        this.logger = logger;
        this.throwable = throwable;
        this.level = level;
        timeStamp = System.currentTimeMillis();
    }

    public Object getMessage() {
        if (message != null) {
            return message;
        } else {
            return getRenderedMessage();
        }
    }

    public String getRenderedMessage() {
        if (renderedMessage == null && message != null) {
            if (message instanceof String) {
                renderedMessage = (String) message;
            } else {
                renderedMessage = message.toString();
            }
        }
        return renderedMessage;
    }

    public String getThreadName() {
        if (threadName == null) {
            threadName = (Thread.currentThread()).getName();
        }
        return threadName;
    }

    public Level getLevel() {
        return level;
    }

    public String getLoggerName() {
        return logger.getName();
    }

    public String[] getThrowableStr() {
        if (throwable == null) {
            return null;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            throwable.printStackTrace(pw);
        } catch (RuntimeException ex) {
            SysLogger.warn("InnerLogger print stack trace error", ex);
        }
        pw.flush();
        LineNumberReader reader = new LineNumberReader(
            new StringReader(sw.toString()));
        ArrayList<String> lines = new ArrayList<String>();
        try {
            String line = reader.readLine();
            while (line != null) {
                lines.add(line);
                line = reader.readLine();
            }
        } catch (IOException ex) {
            if (ex instanceof InterruptedIOException) {
                Thread.currentThread().interrupt();
            }
            lines.add(ex.toString());
        }
        String[] tempRep = new String[lines.size()];
        lines.toArray(tempRep);
        return tempRep;
    }
}
