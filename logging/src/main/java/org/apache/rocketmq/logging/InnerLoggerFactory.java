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

import org.apache.rocketmq.logging.inner.Logger;

public class InnerLoggerFactory extends InternalLoggerFactory {

    @Override
    protected InternalLogger doGetLogger(String name) {
        return new InnerLogger(name);
    }

    @Override
    protected String getLoggerType() {
        return LOGGER_INNER;
    }

    public static class InnerLogger implements InternalLogger {

        private Logger logger;

        public InnerLogger(String name) {
            logger = Logger.getLogger(name);
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public void trace(String var1) {
            logger.trace(var1);
        }

        @Override
        public void trace(String var1, Throwable var2) {
            logger.trace(var1, var2);
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public void debug(String var1) {
            logger.debug(var1);
        }

        @Override
        public void debug(String var1, Throwable var2) {
            logger.debug(var1, var2);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public void info(String var1) {
            logger.info(var1);
        }

        @Override
        public void info(String var1, Throwable var2) {
            logger.info(var1, var2);
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public void warn(String var1) {
            logger.warn(var1);
        }

        @Override
        public void warn(String var1, Throwable var2) {
            logger.warn(var1, var2);
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override
        public void error(String var1) {
            logger.error(var1);
        }

        @Override
        public void error(String var1, Throwable var2) {
            logger.error(var1, var2);
        }

        @Override
        public void trace(String var1, Object var2) {

        }

        @Override
        public void trace(String var1, Object var2, Object var3) {

        }

        @Override
        public void trace(String var1, Object... var2) {

        }

        @Override
        public void debug(String var1, Object var2) {

        }

        @Override
        public void debug(String var1, Object var2, Object var3) {

        }

        @Override
        public void debug(String var1, Object... var2) {

        }

        @Override
        public void info(String var1, Object var2) {

        }

        @Override
        public void info(String var1, Object var2, Object var3) {

        }

        @Override
        public void info(String var1, Object... var2) {

        }

        @Override
        public void warn(String var1, Object var2) {

        }

        @Override
        public void warn(String var1, Object... var2) {

        }

        @Override
        public void warn(String var1, Object var2, Object var3) {

        }

        @Override
        public void error(String var1, Object var2) {

        }

        @Override
        public void error(String var1, Object var2, Object var3) {

        }

        @Override
        public void error(String var1, Object... var2) {

        }

        public Logger getLogger() {
            return logger;
        }
    }
}
