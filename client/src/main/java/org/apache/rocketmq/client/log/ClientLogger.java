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
package org.apache.rocketmq.client.log;

import java.lang.reflect.Method;
import java.net.URL;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientLogger {
    public static final String CLIENT_LOG_ROOT = "rocketmq.client.logRoot";
    public static final String CLIENT_LOG_MAXINDEX = "rocketmq.client.logFileMaxIndex";
    public static final String CLIENT_LOG_LEVEL = "rocketmq.client.logLevel";
    private static Logger log;

    static {
        log = createLogger(LoggerName.CLIENT_LOGGER_NAME);
    }

    private static Logger createLogger(final String loggerName) {
        String logConfigFilePath =
            System.getProperty("rocketmq.client.log.configFile",
                System.getenv("ROCKETMQ_CLIENT_LOG_CONFIGFILE"));
        Boolean isloadconfig =
            Boolean.parseBoolean(System.getProperty("rocketmq.client.log.loadconfig", "true"));

        final String log4JResourceFile =
            System.getProperty("rocketmq.client.log4j.resource.fileName", "log4j_rocketmq_client.xml");

        final String logbackResourceFile =
            System.getProperty("rocketmq.client.logback.resource.fileName", "logback_rocketmq_client.xml");

        String clientLogRoot = System.getProperty(CLIENT_LOG_ROOT, "${user.home}/logs/rocketmqlogs");
        System.setProperty("client.logRoot", clientLogRoot);
        String clientLogLevel = System.getProperty(CLIENT_LOG_LEVEL, "INFO");
        System.setProperty("client.logLevel", clientLogLevel);
        String clientLogMaxIndex = System.getProperty(CLIENT_LOG_MAXINDEX, "10");
        System.setProperty("client.logFileMaxIndex", clientLogMaxIndex);

        if (isloadconfig) {
            try {
                ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
                Class classType = iLoggerFactory.getClass();
                if (classType.getName().equals("org.slf4j.impl.Log4jLoggerFactory")) {
                    Class<?> domconfigurator;
                    Object domconfiguratorobj;
                    domconfigurator = Class.forName("org.apache.log4j.xml.DOMConfigurator");
                    domconfiguratorobj = domconfigurator.newInstance();
                    if (null == logConfigFilePath) {
                        Method configure = domconfiguratorobj.getClass().getMethod("configure", URL.class);
                        URL url = ClientLogger.class.getClassLoader().getResource(log4JResourceFile);
                        configure.invoke(domconfiguratorobj, url);
                    } else {
                        Method configure = domconfiguratorobj.getClass().getMethod("configure", String.class);
                        configure.invoke(domconfiguratorobj, logConfigFilePath);
                    }

                } else if (classType.getName().equals("ch.qos.logback.classic.LoggerContext")) {
                    Class<?> joranConfigurator;
                    Class<?> context = Class.forName("ch.qos.logback.core.Context");
                    Object joranConfiguratoroObj;
                    joranConfigurator = Class.forName("ch.qos.logback.classic.joran.JoranConfigurator");
                    joranConfiguratoroObj = joranConfigurator.newInstance();
                    Method setContext = joranConfiguratoroObj.getClass().getMethod("setContext", context);
                    setContext.invoke(joranConfiguratoroObj, iLoggerFactory);
                    if (null == logConfigFilePath) {
                        URL url = ClientLogger.class.getClassLoader().getResource(logbackResourceFile);
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
        return LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);
    }

    public static Logger getLog() {
        return log;
    }

    public static void setLog(Logger log) {
        ClientLogger.log = log;
    }
}
