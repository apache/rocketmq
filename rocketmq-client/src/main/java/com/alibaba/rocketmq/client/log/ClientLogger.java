package com.alibaba.rocketmq.client.log;

import java.io.File;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

import com.alibaba.rocketmq.common.constant.LoggerName;


public class ClientLogger {
    private static String logFilePath;
    private static Logger log;

    static {
        // 确认日志默认存储路径
        logFilePath =
                System.getProperty("rocketmq.client.log.path", System.getenv("ROCKETMQ_CLIENT_LOG_PATH"));
        if (null == logFilePath) {
            logFilePath = System.getProperty("user.home") + File.separator + "rocketmqlogs";
        }

        // 初始化Logger
        log = createLogger(LoggerName.ClientLoggerName, "rocketmq_client.log");
    }


    private static Logger createLogger(final String loggerName, final String fileName) {
        Logger logger = LoggerFactory.getLogger(loggerName);
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        ch.qos.logback.classic.Logger newLogger = (ch.qos.logback.classic.Logger) logger;
        // Remove all previously added appenders from this logger instance.
        newLogger.detachAndStopAllAppenders();

        // define appender
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<ILoggingEvent>();

        // policy
        TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<ILoggingEvent>();
        rollingPolicy.setContext(loggerContext);
        rollingPolicy.setFileNamePattern(logFilePath + File.separator + fileName + "-%d{yyyy-MM-dd}.log");
        rollingPolicy.setParent(appender);
        rollingPolicy.start();

        // encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%d{yyy-MM-dd HH:mm:ss,GMT+8} %p %t - %m%n");
        encoder.setCharset(Charset.forName("UTF-8"));
        encoder.start();

        // start appender
        appender.setRollingPolicy(rollingPolicy);
        appender.setContext(loggerContext);
        appender.setEncoder(encoder);
        appender.setPrudent(true); // support that multiple JVMs can safely
                                   // write to the same file.
        appender.start();

        newLogger.addAppender(appender);

        // setup level
        newLogger.setLevel(Level.DEBUG);
        // remove the appenders that inherited 'ROOT'.
        newLogger.setAdditive(true);
        return newLogger;
    }


    public static String getLogFilePath() {
        return logFilePath;
    }


    public static void setLogFilePath(String logFilePath) {
        ClientLogger.logFilePath = logFilePath;
    }


    public static Logger getLog() {
        return log;
    }


    public static void setLog(Logger log) {
        ClientLogger.log = log;
    }
}
