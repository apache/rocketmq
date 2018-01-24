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

package org.apache.rocketmq.remoting.netty;


import io.netty.util.internal.logging.InternalLogLevel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class NettyLogger {

    private static AtomicBoolean nettyLoggerSeted = new AtomicBoolean(false);

    public static void initNettyLogger() {
        if (!nettyLoggerSeted.get()) {
            try {
                io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(new NettyBridgeLoggerFactory());
            } catch (Throwable e) {
                //ignore
            }
            nettyLoggerSeted.set(true);
        }
    }

    private static class NettyBridgeLoggerFactory extends io.netty.util.internal.logging.InternalLoggerFactory {
        @Override
        protected io.netty.util.internal.logging.InternalLogger newInstance(String s) {
            return new NettyBridgeLogger(s);
        }
    }

    private static class NettyBridgeLogger implements io.netty.util.internal.logging.InternalLogger {

        private InternalLogger logger = null;

        public NettyBridgeLogger(String name) {
            logger = InternalLoggerFactory.getLogger(name);
        }

        @Override
        public String name() {
            return logger.getName();
        }

        @Override
        public boolean isEnabled(InternalLogLevel internalLogLevel) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                return logger.isDebugEnabled();
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                return logger.isTraceEnabled();
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                return logger.isInfoEnabled();
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                return logger.isWarnEnabled();
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                return logger.isErrorEnabled();
            }
            return false;
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.trace(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.trace(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o, Object o1) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.trace(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o, o1);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object... objects) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.trace(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, objects);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Throwable throwable) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.trace(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, throwable);
            }
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
        public void trace(String var1, Object var2) {
            logger.trace(var1, var2);
        }

        @Override
        public void trace(String var1, Object var2, Object var3) {
            logger.trace(var1, var2, var3);
        }

        @Override
        public void trace(String var1, Object... var2) {
            logger.trace(var1, var2);
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
        public void debug(String var1, Object var2) {
            logger.debug(var1, var2);
        }

        @Override
        public void debug(String var1, Object var2, Object var3) {
            logger.debug(var1, var2, var3);
        }

        @Override
        public void debug(String var1, Object... var2) {
            logger.debug(var1, var2);
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
        public void info(String var1, Object var2) {
            logger.info(var1, var2);
        }

        @Override
        public void info(String var1, Object var2, Object var3) {
            logger.info(var1, var2, var3);
        }

        @Override
        public void info(String var1, Object... var2) {
            logger.info(var1, var2);
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
        public void warn(String var1, Object var2) {
            logger.warn(var1, var2);
        }

        @Override
        public void warn(String var1, Object... var2) {
            logger.warn(var1, var2);
        }

        @Override
        public void warn(String var1, Object var2, Object var3) {
            logger.warn(var1, var2, var3);
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
        public void error(String var1, Object var2) {
            logger.error(var1, var2);
        }

        @Override
        public void error(String var1, Object var2, Object var3) {
            logger.error(var1, var2, var3);
        }

        @Override
        public void error(String var1, Object... var2) {
            logger.error(var1, var2);
        }

        @Override
        public void error(String var1, Throwable var2) {
            logger.error(var1, var2);
        }
    }

}
