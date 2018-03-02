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

package org.apache.rocketmq.logging.inner;

import org.apache.rocketmq.logging.BasicLoggerTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Enumeration;

public class LoggerRepositoryTest extends BasicLoggerTest {

    @Test
    public void testLoggerRepository() {
        Logger.getRepository().setLogLevel(Level.INFO);

        String file = loggingDir + "/repo.log";
        Logger fileLogger = Logger.getLogger("repoLogger");

        Appender myappender = LoggingBuilder.newAppenderBuilder()
            .withDailyFileRollingAppender(file, "'.'yyyy-MM-dd")
            .withName("repoAppender")
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        fileLogger.addAppender(myappender);
        Logger.getLogger("repoLogger").setLevel(Level.INFO);
        Logger repoLogger = Logger.getRepository().exists("repoLogger");
        Assert.assertTrue(repoLogger != null);
        Enumeration currentLoggers = Logger.getRepository().getCurrentLoggers();
        Level logLevel = Logger.getRepository().getLogLevel();
        Assert.assertTrue(logLevel.equals(Level.INFO));
//        Logger.getRepository().shutdown();
    }
}
