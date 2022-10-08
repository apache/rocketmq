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

package org.apache.rocketmq.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jLoggerFactory extends InternalLoggerFactory {

    public Slf4jLoggerFactory() {
        LoggerFactory.getILoggerFactory();
        doRegister();
    }

    @Override
    protected String getLoggerType() {
        return InternalLoggerFactory.LOGGER_SLF4J;
    }

    @Override
    protected InternalLogger getLoggerInstance(String name) {
        return new Slf4jLogger(name);
    }

    @Override
    protected void shutdown() {

    }

    public static class Slf4jLogger implements InternalLogger {
        private static final Pattern PATTERN = Pattern.compile("#.*#");

        private final String loggerSuffix;
        private final Logger defaultLogger;

        private final Map<String, Logger> loggerMap = new HashMap<String, Logger>();

        public Slf4jLogger(String loggerSuffix) {
            this.loggerSuffix = loggerSuffix;
            this.defaultLogger = LoggerFactory.getLogger(loggerSuffix);
        }

        private Logger getLogger() {
            if (loggerSuffix.equals(ACCOUNT_LOGGER_NAME)
                || loggerSuffix.equals(CONSUMER_STATS_LOGGER_NAME)
                || loggerSuffix.equals(COMMERCIAL_LOGGER_NAME)) {
                return defaultLogger;
            }
            String brokerIdentity = InnerLoggerFactory.BROKER_IDENTITY.get();
            if (brokerIdentity == null) {
                Matcher m = PATTERN.matcher(Thread.currentThread().getName());
                if (m.find()) {
                    String match = m.group();
                    brokerIdentity = match.substring(1, match.length() - 1);
                }
            }
            if (InnerLoggerFactory.BROKER_CONTAINER_NAME.equals(brokerIdentity)) {
                return defaultLogger;
            }
            if (brokerIdentity != null) {
                if (!loggerMap.containsKey(brokerIdentity)) {
                    loggerMap.put(brokerIdentity, LoggerFactory.getLogger("#" + brokerIdentity + "#" + loggerSuffix));
                }
                return loggerMap.get(brokerIdentity);
            }
            return defaultLogger;
        }

        @Override
        public String getName() {
            return getLogger().getName();
        }

        @Override
        public void debug(String s) {
            getLogger().debug(s);
        }

        @Override
        public void debug(String s, Object o) {
            getLogger().debug(s, o);
        }

        @Override
        public void debug(String s, Object o, Object o1) {
            getLogger().debug(s, o, o1);
        }

        @Override
        public void debug(String s, Object... objects) {
            getLogger().debug(s, objects);
        }

        @Override
        public void debug(String s, Throwable throwable) {
            getLogger().debug(s, throwable);
        }

        @Override
        public void info(String s) {
            getLogger().info(s);
        }

        @Override
        public void info(String s, Object o) {
            getLogger().info(s, o);
        }

        @Override
        public void info(String s, Object o, Object o1) {
            getLogger().info(s, o, o1);
        }

        @Override
        public void info(String s, Object... objects) {
            getLogger().info(s, objects);
        }

        @Override
        public void info(String s, Throwable throwable) {
            getLogger().info(s, throwable);
        }

        @Override
        public void warn(String s) {
            getLogger().warn(s);
        }

        @Override
        public void warn(String s, Object o) {
            getLogger().warn(s, o);
        }

        @Override
        public void warn(String s, Object... objects) {
            getLogger().warn(s, objects);
        }

        @Override
        public void warn(String s, Object o, Object o1) {
            getLogger().warn(s, o, o1);
        }

        @Override
        public void warn(String s, Throwable throwable) {
            getLogger().warn(s, throwable);
        }

        @Override
        public void error(String s) {
            getLogger().error(s);
        }

        @Override
        public void error(String s, Object o) {
            getLogger().error(s, o);
        }

        @Override
        public void error(String s, Object o, Object o1) {
            getLogger().error(s, o, o1);
        }

        @Override
        public void error(String s, Object... objects) {
            getLogger().error(s, objects);
        }

        @Override
        public void error(String s, Throwable throwable) {
            getLogger().error(s, throwable);
        }
    }
}
