package com.alibaba.rocketmq.client.log;

import java.io.File;
import java.nio.charset.Charset;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.joran.spi.JoranException;
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
        try {
            LoggerContext lc = new LoggerContext();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            // 配置文件已经打包到Client Jar包
            configurator.doConfigure("logback_rocketmq_client.xml");
        }
        catch (Exception e) {
            System.err.println(e);
        }

        final Logger logger = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

        return logger;
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
