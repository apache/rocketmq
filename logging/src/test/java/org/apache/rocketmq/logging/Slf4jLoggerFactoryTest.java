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

public class Slf4jLoggerFactoryTest extends BasicLoggerTest {

    public static final String LOGGER = "Slf4jTestLogger";

    @Before
    public void initLogback() throws JoranException {
        InternalLoggerFactory.setCurrentLoggerType(InternalLoggerFactory.LOGGER_SLF4J);
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
        InternalLogger logger1 = Slf4jLoggerFactory.getLogger(LOGGER);
        InternalLogger logger = InternalLoggerFactory.getLogger(LOGGER);
        Assert.assertTrue(logger.getName().equals(logger1.getName()));
        InternalLogger logger2 = Slf4jLoggerFactory.getLogger(Slf4jLoggerFactoryTest.class);
        Slf4jLoggerFactory.Slf4jLogger logger3 = (Slf4jLoggerFactory.Slf4jLogger) logger2;

        String file = loggingDir + "/logback_test.log";

        logger.info("logback slf4j info Message");
        logger.error("logback slf4j error Message", new RuntimeException("test"));
        logger.debug("logback slf4j debug message");
        logger3.info("logback info message");
        logger3.error("logback error message");
        logger3.info("info {}", "hahahah");
        logger3.warn("warn {}", "hahahah");
        logger3.warn("logger3 warn");
        logger3.error("error {}", "hahahah");
        logger3.debug("debug {}", "hahahah");
        String content = readFile(file);
        System.out.println(content);

        Assert.assertTrue(content.contains("Slf4jLoggerFactoryTest"));
        Assert.assertTrue(content.contains("info"));
        Assert.assertTrue(content.contains("RuntimeException"));
        Assert.assertTrue(!content.contains("debug"));
    }

}
