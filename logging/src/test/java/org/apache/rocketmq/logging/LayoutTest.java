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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.rocketmq.logging.inner.Layout;
import org.apache.rocketmq.logging.inner.Logger;
import org.apache.rocketmq.logging.inner.LoggingBuilder;
import org.apache.rocketmq.logging.inner.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LayoutTest extends BasicloggerTest {

    @Test
    public void testSimpleLayout() {
        Layout layout = LoggingBuilder.newLayoutBuilder().withSimpleLayout().build();
        String format = layout.format(loggingEvent);
        Assert.assertTrue(format.contains("junit"));
    }

    @Test
    public void testDefaultLayout() {
        Layout layout = LoggingBuilder.newLayoutBuilder().withDefaultLayout().build();
        String format = layout.format(loggingEvent);
        Assert.assertTrue(format.contains("createLoggingEvent"));
        Assert.assertTrue(format.contains("createLogging error"));
        Assert.assertTrue(format.contains(Thread.currentThread().getName()));
    }

    @Test
    public void testLogFormat() {
        Log4jLogEvent log4jLogEvent = Log4jLogEvent.newBuilder().setLevel(Level.INFO).setLoggerFqcn(Logger.class.getName()).setLoggerName("test")
            .setMessage(new SimpleMessage("junit test error")).setThreadName(Thread.currentThread().getName()).build();
        PatternLayout log4j2Layout = PatternLayout.newBuilder().withPattern("%d{yyy-MM-dd HH:mm:ss,SSS} %p %c{1}%L - %m%n").build();

        String log4j2Format = new String(log4j2Layout.toByteArray(log4jLogEvent));

        Layout innerLayout = LoggingBuilder.newLayoutBuilder().withDefaultLayout().build();

        LoggingEvent loggingEvent = new LoggingEvent(Logger.class.getName(), logger, org.apache.rocketmq.logging.inner.Level.INFO,
            "junit test error", null);
        String format = innerLayout.format(loggingEvent);

        System.out.println(format);
        System.out.println(log4j2Format);
    }
}
