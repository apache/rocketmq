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

package org.apache.rocketmq.remoting.log;

import java.util.concurrent.ConcurrentHashMap;

public abstract class InternalLoggerFactory {

    public static final String DEFAULT_LOG_CONTEXT = "slf4j";

    private static ConcurrentHashMap<String, InternalLoggerFactory> loggerFactory = new ConcurrentHashMap<String, InternalLoggerFactory>();

    private static ThreadLocal<String> loggerContext = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            return DEFAULT_LOG_CONTEXT;
        }
    };

    public static void setLoggerContext(String context) {
        loggerContext.set(context);
    }

    public static String getLoggerContext() {
        return loggerContext.get();
    }

    public static void clearLoggerContext() {
        loggerContext.remove();
    }

    public static InternalLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static InternalLogger getLogger(String name) {
        String loggerContext = getLoggerContext();
        InternalLoggerFactory internalLoggerFactory = loggerFactory.get(loggerContext);
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactory.get(DEFAULT_LOG_CONTEXT);
        }
        return internalLoggerFactory.doGetLogger(name);
    }

    static {
        try {
            new Slf4jLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
    }

    public InternalLoggerFactory() {
        doRegister();
    }

    protected void doRegister() {
        String context = getContext();
        if (loggerFactory.get(context) != null) {
            return;
        }
        loggerFactory.put(context, this);
    }

    protected abstract InternalLogger doGetLogger(String name);

    protected abstract String getContext();
}
