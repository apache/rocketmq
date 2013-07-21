/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
//import ch.qos.logback.classic.LoggerContext;
//import ch.qos.logback.classic.joran.JoranConfigurator;


public class ClientLogger {
    private static Logger log;

    static {
        // 初始化Logger
        log = createLogger(LoggerName.ClientLoggerName);
    }


    private static Logger createLogger(final String loggerName) {
//        String logConfigFilePath =
//                System.getProperty("rocketmq.client.log.configFile",
//                    System.getenv("ROCKETMQ_CLIENT_LOG_CONFIGFILE"));
//
//        try {
//            LoggerContext lc = new LoggerContext();
//            JoranConfigurator configurator = new JoranConfigurator();
//            configurator.setContext(lc);
//            lc.reset();
//
//            if (null == logConfigFilePath) {
//                // 如果应用没有配置，则使用jar包内置配置
//                URL url = ClientLogger.class.getClassLoader().getResource("logback_rocketmq_client.xml");
//                configurator.doConfigure(url);
//            }
//            else {
//                configurator.doConfigure(logConfigFilePath);
//            }
//
//            return lc.getLogger(LoggerName.ClientLoggerName);
//        }
//        catch (Exception e) {
//            System.err.println(e);
//        }

        return LoggerFactory.getLogger(LoggerName.ClientLoggerName);
    }


    public static Logger getLog() {
        return log;
    }


    public static void setLog(Logger log) {
        ClientLogger.log = log;
    }
}
