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

import org.apache.rocketmq.logging.inner.Appender;
import org.apache.rocketmq.logging.inner.Level;
import org.apache.rocketmq.logging.inner.Logger;
import org.apache.rocketmq.logging.inner.LoggingBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

@RunWith(JUnit4.class)
public class LoggerTest extends BasicloggerTest{

    @Test
    public void testInnerConsole(){
        PrintStream out = System.out;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(byteArrayOutputStream));

        Appender consoleAppender = LoggingBuilder.newAppenderBuilder()
            .withConsoleAppender(LoggingBuilder.SYSTEM_OUT)
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        Logger consoleLogger = Logger.getLogger("ConsoleLogger");
        consoleLogger.addAppender(consoleAppender);
        consoleLogger.setLevel(Level.INFO);


        Logger.getLogger("ConsoleLogger").info("console info Message");
        Logger.getLogger("ConsoleLogger").error("console error Message",new RuntimeException());
        Logger.getLogger("ConsoleLogger").debug("console debug message");
        System.setOut(out);
        consoleAppender.close();

        String result = new String(byteArrayOutputStream.toByteArray());

        Assert.assertTrue(result.contains("info"));
        Assert.assertTrue(result.contains("RuntimeException"));
        Assert.assertTrue(!result.contains("debug"));
    }

    @Test
    public void testInnerFile() throws IOException {
        String file = loggingDir+"/logger.log";

        Logger fileLogger = Logger.getLogger("fileLogger");

        Appender myappender = LoggingBuilder.newAppenderBuilder()
            .withDailyFileRollingAppender(file, "'.'yyyy-MM-dd")
            .withName("myappender")
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        fileLogger.addAppender(myappender);

        Logger.getLogger("fileLogger").setLevel(Level.INFO);

        Logger.getLogger("fileLogger").info("fileLogger info Message");
        Logger.getLogger("fileLogger").error("fileLogger error Message",new RuntimeException());
        Logger.getLogger("fileLogger").debug("fileLogger debug message");

        myappender.close();

        String content = readFile(file);

        System.out.println(content);

        Assert.assertTrue(content.contains("info"));
        Assert.assertTrue(content.contains("RuntimeException"));
        Assert.assertTrue(!content.contains("debug"));
    }
}
