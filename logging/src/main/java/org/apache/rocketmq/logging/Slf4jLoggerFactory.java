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

        private Logger logger = null;

        public Slf4jLogger(String name) {
            logger = LoggerFactory.getLogger(name);
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public void debug(String s) {
            logger.debug(s);
        }

        @Override
        public void debug(String s, Object o) {
            logger.debug(s, o);
        }

        @Override
        public void debug(String s, Object o, Object o1) {
            logger.debug(s, o, o1);
        }

        @Override
        public void debug(String s, Object... objects) {
            logger.debug(s, objects);
        }

        @Override
        public void debug(String s, Throwable throwable) {
            logger.debug(s, throwable);
        }

        @Override
        public void info(String s) {
            logger.info(s);
        }

        @Override
        public void info(String s, Object o) {
            logger.info(s, o);
        }

        @Override
        public void info(String s, Object o, Object o1) {
            logger.info(s, o, o1);
        }

        @Override
        public void info(String s, Object... objects) {
            logger.info(s, objects);
        }

        @Override
        public void info(String s, Throwable throwable) {
            logger.info(s, throwable);
        }

        @Override
        public void warn(String s) {
            logger.warn(s);
        }

        @Override
        public void warn(String s, Object o) {
            logger.warn(s, o);
        }

        @Override
        public void warn(String s, Object... objects) {
            logger.warn(s, objects);
        }

        @Override
        public void warn(String s, Object o, Object o1) {
            logger.warn(s, o, o1);
        }

        @Override
        public void warn(String s, Throwable throwable) {
            logger.warn(s, throwable);
        }

        @Override
        public void error(String s) {
            logger.error(s);
        }

        @Override
        public void error(String s, Object o) {
            logger.error(s, o);
        }

        @Override
        public void error(String s, Object o, Object o1) {
            logger.error(s, o, o1);
        }

        @Override
        public void error(String s, Object... objects) {
            logger.error(s, objects);
        }

        @Override
        public void error(String s, Throwable throwable) {
            logger.error(s, throwable);
        }
    }
}
