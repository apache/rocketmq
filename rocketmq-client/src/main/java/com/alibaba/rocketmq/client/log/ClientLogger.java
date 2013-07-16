package com.alibaba.rocketmq.client.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.alibaba.rocketmq.common.constant.LoggerName;


public class ClientLogger {
    private static Logger log;

    static {
        String logConfigFilePath =
                System.getProperty("rocketmq.client.log.configFile",
                    System.getenv("ROCKETMQ_CLIENT_LOG_CONFIGFILE"));
        if (null == logConfigFilePath) {
            // 如果应用没有配置，则使用jar包内置配置
            logConfigFilePath = "logback_rocketmq_client.xml";
        }

        // 初始化Logger
        log = createLogger(LoggerName.ClientLoggerName, logConfigFilePath);
    }


    private static Logger createLogger(final String loggerName, final String logConfigFile) {
        try {
            LoggerContext lc = new LoggerContext();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            // 配置文件已经打包到Client Jar包
            configurator.doConfigure(logConfigFile);
            return lc.getLogger(LoggerName.ClientLoggerName);
        }
        catch (Exception e) {
            System.err.println(e);
        }

        return LoggerFactory.getLogger(LoggerName.ClientLoggerName);
    }


    public static Logger getLog() {
        return log;
    }


    public static void setLog(Logger log) {
        ClientLogger.log = log;
    }
}
