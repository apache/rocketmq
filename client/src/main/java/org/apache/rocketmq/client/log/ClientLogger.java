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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.logging.InnerLoggerFactory;
import org.apache.rocketmq.remoting.log.InternalLogger;
import org.apache.rocketmq.remoting.log.InternalLoggerFactory;
import org.apache.rocketmq.common.logging.internal.Appender;
import org.apache.rocketmq.common.logging.internal.Layout;
import org.apache.rocketmq.common.logging.internal.Level;
import org.apache.rocketmq.common.logging.internal.Logger;
import org.apache.rocketmq.common.logging.internal.LoggingBuilder;


public class ClientLogger {

    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String CLIENT_LOG_MAXINDEX = "rocketmq.client.logFileMaxIndex";
    public static final String CLIENT_LOG_FILESIZE = "rocketmq.client.logFileMaxSize";
    public static final String CLIENT_LOG_LEVEL = "rocketmq.client.logLevel";
    public static final String CLIENT_LOG_FILENAME = "rocketmq.client.logFileName";
    public static final String CLIENT_LOG_ASYNC_QUEUESIZE = "rocketmq.client.logAsyncQueueSize";
    public static final String ROCKETMQ_CLIENT_APPENDER_NAME = "RocketmqClientAppender";

    private static InternalLogger log;

    private static Appender rocketmqClientAppender = null;

    static {
        try {
            new InnerLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
    }

    private static synchronized void createClientAppender() {
        String clientLogRoot = System.getProperty(CLIENT_LOG_ROOT, System.getProperty("user.home") + "/logs/rocketmqlogs");
        String clientLogMaxIndex = System.getProperty(CLIENT_LOG_MAXINDEX, "10");
        String clientLogFileName = System.getProperty(CLIENT_LOG_FILENAME, "rocketmq_client.log");
        String maxFileSize = System.getProperty(CLIENT_LOG_FILESIZE, "1073741824");
        String asyncQueueSize = System.getProperty(CLIENT_LOG_ASYNC_QUEUESIZE, "1024");

        String logFileName = clientLogRoot + "/" + clientLogFileName;

        int maxFileIndex = Integer.parseInt(clientLogMaxIndex);
        int queueSize = Integer.parseInt(asyncQueueSize);

        Layout layout = LoggingBuilder.newLayoutBuilder().withDefaultLayout().build();

        rocketmqClientAppender = LoggingBuilder.newAppenderBuilder()
            .withRollingFileAppender(logFileName, maxFileSize, maxFileIndex)
            .withAsync(false, queueSize).withName(ROCKETMQ_CLIENT_APPENDER_NAME).withLayout(layout).build();
    }

    private static InternalLogger createLogger(final String loggerName) {
        String clientLogLevel = System.getProperty(CLIENT_LOG_LEVEL, "INFO");
        InternalLoggerFactory.setLoggerContext(InnerLoggerFactory.LOG_CONTEXT_INTERNAL);

        InternalLogger logger = InternalLoggerFactory.getLogger(loggerName);
        InnerLoggerFactory.InnerLogger innerLogger = (InnerLoggerFactory.InnerLogger) logger;
        Logger realLogger = innerLogger.getLogger();

        if (rocketmqClientAppender == null) {
            createClientAppender();
        }

        realLogger.addAppender(rocketmqClientAppender);
        realLogger.setLevel(Level.toLevel(clientLogLevel));

        InternalLoggerFactory.clearLoggerContext();
        return logger;
    }

    public static InternalLogger getLog() {
        if (log == null) {
            log = createLogger(LoggerName.CLIENT_LOGGER_NAME);
            return log;
        } else {
            return log;
        }
    }

    public static void setLog(InternalLogger log) {
        ClientLogger.log = log;
    }
}
