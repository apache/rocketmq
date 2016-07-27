/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.client.log;

import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URL;


/**
 * @author shijia.wxr
 */
public class ClientLogger {
    private static Logger log;
    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String CLIENT_LOG_MAXINDEX = "rocketmq.client.logFileMaxIndex";
    public static final String CLIENT_LOG_LEVEL = "rocketmq.client.logLevel";

    static {
        log = createLogger(LoggerName.ClientLoggerName);
    }


    private static Logger createLogger(final String loggerName) {
        String logConfigFilePath =
                System.getProperty("rocketmq.client.log.configFile",
                        System.getenv("ROCKETMQ_CLIENT_LOG_CONFIGFILE"));
        Boolean isloadconfig =
                Boolean.parseBoolean(System.getProperty("rocketmq.client.log.loadconfig", "true"));

        final String log4j_resource_file =
                System.getProperty("rocketmq.client.log4j.resource.fileName", "log4j_rocketmq_client.xml");

        final String logback_resource_file =
                System
                        .getProperty("rocketmq.client.logback.resource.fileName", "logback_rocketmq_client.xml");

        String clientLogRoot = System.getProperty(CLIENT_LOG_ROOT,"${user.home}/logs/rocketmqlogs");
        System.setProperty("client.logRoot",clientLogRoot);
        String clientLogLevel = System.getProperty(CLIENT_LOG_LEVEL,"INFO");
        System.setProperty("client.logLevel",clientLogLevel);
        String clientLogMaxIndex = System.getProperty(CLIENT_LOG_MAXINDEX,"10");
        System.setProperty("client.logFileMaxIndex",clientLogMaxIndex);

        if (isloadconfig) {
            try {
                ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
                Class classType = iLoggerFactory.getClass();
                if (classType.getName().equals("org.slf4j.impl.Log4jLoggerFactory")) {
                    Class<?> DOMConfigurator = null;
                    Object DOMConfiguratorObj = null;
                    DOMConfigurator = Class.forName("org.apache.log4j.xml.DOMConfigurator");
                    DOMConfiguratorObj = DOMConfigurator.newInstance();
                    if (null == logConfigFilePath) {
                        Method configure = DOMConfiguratorObj.getClass().getMethod("configure", URL.class);
                        URL url = ClientLogger.class.getClassLoader().getResource(log4j_resource_file);
                        configure.invoke(DOMConfiguratorObj, url);
                    } else {
                        Method configure = DOMConfiguratorObj.getClass().getMethod("configure", String.class);
                        configure.invoke(DOMConfiguratorObj, logConfigFilePath);
                    }

                } else if (classType.getName().equals("ch.qos.logback.classic.LoggerContext")) {
                    Class<?> joranConfigurator = null;
                    Class<?> context = Class.forName("ch.qos.logback.core.Context");
                    Object joranConfiguratoroObj = null;
                    joranConfigurator = Class.forName("ch.qos.logback.classic.joran.JoranConfigurator");
                    joranConfiguratoroObj = joranConfigurator.newInstance();
                    Method setContext = joranConfiguratoroObj.getClass().getMethod("setContext", context);
                    setContext.invoke(joranConfiguratoroObj, iLoggerFactory);
                    if (null == logConfigFilePath) {
                        URL url = ClientLogger.class.getClassLoader().getResource(logback_resource_file);
                        Method doConfigure =
                                joranConfiguratoroObj.getClass().getMethod("doConfigure", URL.class);
                        doConfigure.invoke(joranConfiguratoroObj, url);
                    } else {
                        Method doConfigure =
                                joranConfiguratoroObj.getClass().getMethod("doConfigure", String.class);
                        doConfigure.invoke(joranConfiguratoroObj, logConfigFilePath);
                    }

                }
            } catch (Exception e) {
                System.err.println(e);
            }
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
