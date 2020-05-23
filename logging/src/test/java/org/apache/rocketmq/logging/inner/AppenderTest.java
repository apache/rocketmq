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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class AppenderTest extends BasicLoggerTest {

    @Test
    public void testConsole() {
        SysLogger.setQuietMode(false);
        SysLogger.setInternalDebugging(true);
        PrintStream out = System.out;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(byteArrayOutputStream));

        Appender consoleAppender = LoggingBuilder.newAppenderBuilder()
            .withConsoleAppender(LoggingBuilder.SYSTEM_OUT)
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        LoggingBuilder.ConsoleAppender consoleAppender1 = (LoggingBuilder.ConsoleAppender) consoleAppender;
        String target = consoleAppender1.getTarget();
        Assert.assertTrue(target.equals(LoggingBuilder.SYSTEM_OUT));

        Layout layout = consoleAppender.getLayout();
        Assert.assertTrue(layout instanceof LoggingBuilder.DefaultLayout);

        Logger consoleLogger = Logger.getLogger("ConsoleLogger");
        consoleLogger.setAdditivity(false);
        consoleLogger.addAppender(consoleAppender);
        consoleLogger.setLevel(Level.INFO);

        Logger.getRootLogger().addAppender(consoleAppender);
        Logger.getLogger(AppenderTest.class).info("this is a AppenderTest log");

        Logger.getLogger("ConsoleLogger").info("console info Message");
        Logger.getLogger("ConsoleLogger").error("console error Message", new RuntimeException());
        Logger.getLogger("ConsoleLogger").debug("console debug message");
        System.setOut(out);
        consoleAppender.close();

        String result = new String(byteArrayOutputStream.toByteArray());

        Assert.assertTrue(result.contains("info"));
        Assert.assertTrue(result.contains("RuntimeException"));
        Assert.assertTrue(!result.contains("debug"));
        Assert.assertTrue(result.contains("AppenderTest"));
    }

    @Test
    public void testInnerFile() throws IOException {
        String file = loggingDir + "/logger.log";

        Logger fileLogger = Logger.getLogger("fileLogger");

        Appender myappender = LoggingBuilder.newAppenderBuilder()
            .withDailyFileRollingAppender(file, "'.'yyyy-MM-dd")
            .withName("myappender")
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        fileLogger.addAppender(myappender);

        Logger.getLogger("fileLogger").setLevel(Level.INFO);

        Logger.getLogger("fileLogger").info("fileLogger info Message");
        Logger.getLogger("fileLogger").error("fileLogger error Message", new RuntimeException());
        Logger.getLogger("fileLogger").debug("fileLogger debug message");

        myappender.close();

        String content = readFile(file);

        System.out.println(content);

        Assert.assertTrue(content.contains("info"));
        Assert.assertTrue(content.contains("RuntimeException"));
        Assert.assertTrue(!content.contains("debug"));
    }



    @Test
    public void asyncAppenderTest() {
        Appender appender = LoggingBuilder.newAppenderBuilder().withAsync(false, 1024)
            .withConsoleAppender(LoggingBuilder.SYSTEM_OUT)
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();
        Assert.assertTrue(appender instanceof LoggingBuilder.AsyncAppender);
        LoggingBuilder.AsyncAppender asyncAppender = (LoggingBuilder.AsyncAppender) appender;
        Assert.assertTrue(!asyncAppender.getBlocking());
        Assert.assertTrue(asyncAppender.getBufferSize() > 0);
    }

    @Test
    public void testWriteAppender() {
        LoggingBuilder.WriterAppender writerAppender = new LoggingBuilder.WriterAppender();
        writerAppender.setImmediateFlush(true);
        Assert.assertTrue(writerAppender.getImmediateFlush());
    }

    @Test
    public void testFileAppender() throws IOException {
        LoggingBuilder.FileAppender fileAppender = new LoggingBuilder.FileAppender(
            new LoggingBuilder.SimpleLayout(), loggingDir + "/simple.log", true);
        fileAppender.setBufferSize(1024);
        int bufferSize = fileAppender.getBufferSize();
        boolean bufferedIO = fileAppender.getBufferedIO();
        Assert.assertTrue(!bufferedIO);
        Assert.assertTrue(bufferSize > 0);
        Assert.assertTrue(fileAppender.getAppend());

        LoggingBuilder.RollingFileAppender rollingFileAppender = new LoggingBuilder.RollingFileAppender();
        rollingFileAppender.setImmediateFlush(true);
        rollingFileAppender.setMaximumFileSize(1024 * 1024);
        rollingFileAppender.setMaxBackupIndex(10);
        rollingFileAppender.setAppend(true);
        rollingFileAppender.setFile(loggingDir + "/rolling_file.log");
        rollingFileAppender.setName("myRollingFileAppender");

        rollingFileAppender.activateOptions();

        Assert.assertTrue(rollingFileAppender.getMaximumFileSize() > 0);
        Assert.assertTrue(rollingFileAppender.getMaxBackupIndex() == 10);
    }

    @Test
    public void testDailyRollingAppender() {
        LoggingBuilder.DailyRollingFileAppender dailyRollingFileAppender = new LoggingBuilder.DailyRollingFileAppender();
        dailyRollingFileAppender.setFile(loggingDir + "/daily.log");
        dailyRollingFileAppender.setName("dailyAppender");
        dailyRollingFileAppender.setAppend(true);
        dailyRollingFileAppender.setDatePattern("'.'yyyy-mm-dd");
        String datePattern = dailyRollingFileAppender.getDatePattern();
        Assert.assertTrue(datePattern != null);
        dailyRollingFileAppender.activateOptions();
    }

}


