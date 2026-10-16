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
package org.apache.rocketmq.tools;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.logging.JoranConfiguratorExt;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Logger;
import org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext;
import org.apache.rocketmq.logging.ch.qos.logback.core.status.Status;
import org.apache.rocketmq.logging.ch.qos.logback.core.status.StatusListener;
import org.apache.rocketmq.logging.ch.qos.logback.core.status.StatusManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Loads the logging configuration the distribution ships for the {@code mqadmin} tool (installed as
 * {@code conf/rmq.tools.logback.xml} and selected by {@code distribution/bin/mqadmin}) with the same
 * configurator the runtime uses, so that a configuration mistake — such as an {@code appender-ref}
 * to an appender the file does not define — fails the build instead of only showing up as logback
 * errors on every invocation.
 */
public class LogbackConfigurationTest {

    private static final String[] TOOLS_LOGGERS = {"RocketmqCommon", "RocketmqStore", "RocketmqRemoting"};

    @Test
    public void testShippedToolsLogbackConfigResolvesEveryAppenderRef() throws Exception {
        URL url = LogbackConfigurationTest.class.getClassLoader().getResource("rmq.tools.logback.xml");
        Assert.assertNotNull("rmq.tools.logback.xml must be on the test classpath", url);

        LoggerContext context = new LoggerContext();
        CollectingStatusManager statusManager = new CollectingStatusManager();
        context.setStatusManager(statusManager);
        context.start();

        JoranConfiguratorExt configurator = new JoranConfiguratorExt();
        configurator.setContext(context);
        configurator.doConfigure0(url);

        Assert.assertTrue("the shipped configuration must not report errors: " + statusManager.errorMessages(),
            statusManager.errorMessages().isEmpty());

        for (String loggerName : TOOLS_LOGGERS) {
            Logger logger = context.getLogger(loggerName);
            Assert.assertTrue(loggerName + " must be attached to an appender (logback status: "
                    + statusManager.statusMessages() + ")",
                logger.iteratorForAppenders().hasNext());
        }
    }

    private static class CollectingStatusManager implements StatusManager {
        private final List<Status> statusList = new ArrayList<>();
        private final List<StatusListener> statusListenerList = new ArrayList<>();

        @Override
        public void add(Status status) {
            statusList.add(status);
        }

        @Override
        public List<Status> getCopyOfStatusList() {
            return new ArrayList<>(statusList);
        }

        @Override
        public int getCount() {
            return statusList.size();
        }

        @Override
        public boolean add(StatusListener listener) {
            return statusListenerList.add(listener);
        }

        @Override
        public void remove(StatusListener listener) {
            statusListenerList.remove(listener);
        }

        @Override
        public void clear() {
            statusList.clear();
        }

        @Override
        public List<StatusListener> getCopyOfStatusListenerList() {
            return new ArrayList<>(statusListenerList);
        }

        List<String> statusMessages() {
            List<String> messages = new ArrayList<>();
            for (Status status : statusList) {
                messages.add("[" + status.getLevel() + "] " + status.getMessage());
            }
            return messages;
        }

        List<String> errorMessages() {
            List<String> messages = new ArrayList<>();
            for (Status status : statusList) {
                if (status.getLevel() == Status.ERROR) {
                    messages.add(status.getMessage());
                }
            }
            return messages;
        }
    }
}
