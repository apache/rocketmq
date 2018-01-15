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

package org.apache.rocketmq.client.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AsyncAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.lang.reflect.Method;

public class Log4j2Helper {

    private static final int ASYNC_APPENDER_QUEUE_SIZE = 1024;

    private static final String APPENDER_FILE_SIZE = "100MB";

    private static final int ASYNC_APPENDER_SHUTDOWN_TIMEOUT = 3;

    private static final String LOG_LAYOUT_PATTERN = "%d{yyy-MM-dd HH:mm:ss,GMT+8} %p %t - %m%n";

    private static Method getMethodWithOnlyName(Class clazz, String name) {
        Method[] declaredMethods = clazz.getDeclaredMethods();
        for (Method declaredMethod : declaredMethods) {
            if (declaredMethod.getName().equals(name)) {
                return declaredMethod;
            }
        }
        return null;
    }

    private static DefaultRolloverStrategy createRolloverStrategy(String max, final Configuration config) {
        Method createMethod = getMethodWithOnlyName(DefaultRolloverStrategy.class, "createStrategy");
        if (createMethod != null) {
            createMethod.setAccessible(true);
            Class<?>[] parameterTypes = createMethod.getParameterTypes();
            try {
                if (parameterTypes.length == 5) {
                    return (DefaultRolloverStrategy) createMethod.invoke(null, max, "1", null, "1", config);
                } else {
                    return (DefaultRolloverStrategy) createMethod.invoke(null, max, "1", null, "1", null, true, config);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        }
        return null;
    }

    private static AbstractAppender createAsyncAppender(AbstractAppender appender, boolean blocking, int queueSize, Configuration config) {
        String refAppenderName = appender.getName();
        AppenderRef ref = AppenderRef.createAppenderRef(refAppenderName, null, null);
        AppenderRef[] refs1 = new AppenderRef[]{ref};
        String asyncAppenderName = "Async" + refAppenderName;

        Method createMethod = getMethodWithOnlyName(AsyncAppender.class, "createAppender");

        if (createMethod == null) {
            return appender;
        }
        AsyncAppender asyncAppender = null;
        try {
            if (createMethod.getParameterTypes().length == 9) {
                asyncAppender = (AsyncAppender) createMethod.invoke(null, refs1, refAppenderName, blocking, queueSize, asyncAppenderName, false, null, config, false);
            } else {
                asyncAppender = (AsyncAppender) createMethod.invoke(null, refs1, refAppenderName, blocking, ASYNC_APPENDER_SHUTDOWN_TIMEOUT,
                    queueSize, asyncAppenderName, false, null, config, false);
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        if (asyncAppender != null) {
            asyncAppender.start();
            config.addAppender(asyncAppender);
            return asyncAppender;
        } else {
            return asyncAppender;
        }
    }

    public static void addClientLogger(String filePath, String level, String logFileMax, boolean async) {
        Level logLevel = Level.valueOf(level);
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        Layout layout = PatternLayout.newBuilder().withPattern(LOG_LAYOUT_PATTERN).withConfiguration(config).build();

        String appenderName = "RocketmqClientAppender";

        String fileName = filePath + "/rocketmq_client.log";
        String filePattern = filePath + "/rocketmq_client-%d{yyyy-MM-dd}-%i.log";

        DefaultRolloverStrategy defaultRolloverStrategy = createRolloverStrategy(logFileMax, config);

        TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.createPolicy("1", "false");
        SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(APPENDER_FILE_SIZE);

        CompositeTriggeringPolicy triggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);

        RollingFileAppender rollingFileAppender = RollingFileAppender.createAppender(fileName, filePattern, "true",
            appenderName, "true", "4096", "true", triggeringPolicy, defaultRolloverStrategy,
            layout, null, "false", null, null, config);

        rollingFileAppender.start();
        config.addAppender(rollingFileAppender);

        AppenderRef[] refs = null;

        if (async) {
            AbstractAppender asyncAppender = createAsyncAppender(rollingFileAppender, false, ASYNC_APPENDER_QUEUE_SIZE, config);
            AppenderRef asyncRef = AppenderRef.createAppenderRef(asyncAppender.getName(), null, null);
            refs = new AppenderRef[]{asyncRef};
        } else {
            AppenderRef ref = AppenderRef.createAppenderRef(appenderName, null, null);
            refs = new AppenderRef[]{ref};
        }

        LoggerConfig clientConfig = LoggerConfig.createLogger("false", logLevel, "RocketmqClient", "true", refs, null, config, null);
        clientConfig.addAppender(rollingFileAppender, null, null);
        config.addLogger("RocketmqClient", clientConfig);

        LoggerConfig commonConfig = LoggerConfig.createLogger("false", logLevel, "RocketmqCommon", "true", refs, null, config, null);
        commonConfig.addAppender(rollingFileAppender, null, null);
        config.addLogger("RocketmqCommon", commonConfig);

        LoggerConfig remotingConfig = LoggerConfig.createLogger("false", logLevel, "RocketmqRemoting", "true", refs, null, config, null);
        remotingConfig.addAppender(rollingFileAppender, null, null);
        config.addLogger("RocketmqRemoting", remotingConfig);

        ctx.updateLoggers();
    }
}
