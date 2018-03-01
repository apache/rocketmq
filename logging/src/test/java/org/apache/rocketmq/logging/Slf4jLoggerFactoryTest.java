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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.joran.spi.JoranException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class Slf4jLoggerFactoryTest extends BasicloggerTest {

    public static final String LOGGER = "Slf4jTestLogger";

    @Before
    public void initLogback() throws JoranException {
        System.setProperty("loggingDir", loggingDir);
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        JoranConfigurator joranConfigurator = new JoranConfigurator();
        joranConfigurator.setContext((Context) iLoggerFactory);
        URL logbackConfigFile = Slf4jLoggerFactoryTest.class.getClassLoader().getResource("logback_test.xml");
        if (logbackConfigFile == null) {
            throw new RuntimeException("can't find logback_test.xml");
        } else {
            joranConfigurator.doConfigure(logbackConfigFile);
        }
    }

    @Test
    public void testSlf4j() throws IOException {
        InternalLogger logger = InternalLoggerFactory.getLogger(LOGGER);
        String file = loggingDir + "/logback_test.log";

        logger.info("logback slf4j info Message");
        logger.error("logback slf4j error Message", new RuntimeException());
        logger.debug("logback slf4j debug message");

        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        LoggerContext context = (LoggerContext) iLoggerFactory;
        context.getLogger(LOGGER).detachAndStopAllAppenders();

        String content = readFile(file);

        System.out.println(content);

        Assert.assertTrue(content.contains("info"));
        Assert.assertTrue(content.contains("RuntimeException"));
        Assert.assertTrue(!content.contains("debug"));
    }

}
