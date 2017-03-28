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
package org.apache.rocketmq.common.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

public class LogUtils {

    /**
     * config logback dynamically from provided file path, otherwise try to config from resource file
     * @param logConfigFilePath specified logback configuration file path
     * @param logbackResourceFile specified logback configuration resource file, which will only be respected when logConfigFilePath is null
     * @param iLoggerFactory
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static void configLogback(String logConfigFilePath, String logbackResourceFile, ILoggerFactory iLoggerFactory) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> joranConfigurator;
        Class<?> context = Class.forName("ch.qos.logback.core.Context");
        Object joranConfiguratoroObj;
        joranConfigurator = Class.forName("ch.qos.logback.classic.joran.JoranConfigurator");
        joranConfiguratoroObj = joranConfigurator.newInstance();
        Method setContext = joranConfiguratoroObj.getClass().getMethod("setContext", context);
        setContext.invoke(joranConfiguratoroObj, iLoggerFactory);
        if (null == logConfigFilePath) {
            if (logbackResourceFile != null) {
                URL url = LogUtils.class.getClassLoader().getResource(logbackResourceFile);
                Method doConfigure = joranConfiguratoroObj.getClass().getMethod("doConfigure", URL.class);
                doConfigure.invoke(joranConfiguratoroObj, url);
            }
        } else {
            Method doConfigure = joranConfiguratoroObj.getClass().getMethod("doConfigure", String.class);
            doConfigure.invoke(joranConfiguratoroObj, logConfigFilePath);
        }
    }

    /**
     * config log4j dynamically from provided file path, otherwise try to config from resource file. Notice that the log4j configuration file should be in xml format
     * @param logConfigFilePath specified log4j configuration file path
     * @param log4JResourceFile specified log4j configuration resource file, which will only be respected when logConfigFilePath is null
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static void configLog4j(String logConfigFilePath, String log4JResourceFile) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> domconfigurator;
        Object domconfiguratorobj;
        domconfigurator = Class.forName("org.apache.log4j.xml.DOMConfigurator");
        domconfiguratorobj = domconfigurator.newInstance();
        if (null == logConfigFilePath) {
            if (log4JResourceFile != null) {
                Method configure = domconfiguratorobj.getClass().getMethod("configure", URL.class);
                URL url = LogUtils.class.getClassLoader().getResource(log4JResourceFile);
                configure.invoke(domconfiguratorobj, url);
            }
        } else {
            Method configure = domconfiguratorobj.getClass().getMethod("configure", String.class);
            configure.invoke(domconfiguratorobj, logConfigFilePath);
        }
    }

    public static void configLog4j2(String logConfigFilePath,
        String log4J2ResourceFile) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Class<?> joranConfigurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
        Method initialize = joranConfigurator.getDeclaredMethod("initialize", String.class, String.class);
        if (null == logConfigFilePath) {
            initialize.invoke(joranConfigurator, "log4j2", log4J2ResourceFile);
        } else {
            initialize.invoke(joranConfigurator, "log4j2", logConfigFilePath);
        }
    }


    public static void loadLoggerConfig(String logConfigFilePath, String log4JResourceFile, String log4J2ResourceFile,
        String logbackResourceFile) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        Class classType = iLoggerFactory.getClass();
        if (classType.getName().equals("org.slf4j.impl.Log4jLoggerFactory")) {
            configLog4j(logConfigFilePath, log4JResourceFile);
        } else if (classType.getName().equals("ch.qos.logback.classic.LoggerContext")) {
            configLogback(logConfigFilePath, logbackResourceFile, iLoggerFactory);
        }
        else if (classType.getName().equals("org.apache.logging.slf4j.Log4jLoggerFactory")) {
            configLog4j2(logConfigFilePath, log4J2ResourceFile);
        }
    }


}
