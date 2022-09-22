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

package org.apache.rocketmq.logging.dynamic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.rocketmq.logging.dynamic.LogConstant.LOGGER_LIST_IS_NULL;
import static org.apache.rocketmq.logging.dynamic.LogConstant.LOGGER_NOT_EXSIT;
import static org.apache.rocketmq.logging.dynamic.LogConstant.LOGGER_TYPE_UNKNOWN;
import static org.apache.rocketmq.logging.dynamic.LogConstant.PARAMETER_TYPE_ERROR;


/**
 * method call processing unit
 */
public class MethodInvokerProcessUnit extends AbstractProcessUnitImpl {
    private final Logger log = LoggerFactory.getLogger(MethodInvokerProcessUnit.class);
    
    @Override
    public String setLogLevel(JSONArray data) {
        log.info("setLogLevel: data={}", data);
        List<LoggerBean> loggerList = JSON.parseArray(data.toString(), LoggerBean.class);
        if (CollectionUtils.isEmpty(loggerList)) {
            throw new RuntimeException(LOGGER_LIST_IS_NULL);
        }
        for (LoggerBean loggerbean : loggerList) {
            Object logger = loggerMap.get(loggerbean.getName());
            if (null == logger) {
                throw new RuntimeException(LOGGER_NOT_EXSIT);
            }
            if (logFrameworkType == LogFrameworkType.LOG4J) {
                org.apache.log4j.Logger targetLogger = (org.apache.log4j.Logger) logger;
                org.apache.log4j.Level targetLevel = org.apache.log4j.Level.toLevel(loggerbean.getLevel());
                targetLogger.setLevel(targetLevel);
            } else if (logFrameworkType == LogFrameworkType.LOGBACK) {
                ch.qos.logback.classic.Logger targetLogger = (ch.qos.logback.classic.Logger) logger;
                ch.qos.logback.classic.Level targetLevel = ch.qos.logback.classic.Level.toLevel(loggerbean.getLevel());
                targetLogger.setLevel(targetLevel);
            } else if (logFrameworkType == LogFrameworkType.LOG4J2) {
                org.apache.logging.log4j.core.config.LoggerConfig loggerConfig = (org.apache.logging.log4j.core.config.LoggerConfig) logger;
                org.apache.logging.log4j.Level targetLevel = org.apache.logging.log4j.Level.toLevel(loggerbean.getLevel());
                loggerConfig.setLevel(targetLevel);
                org.apache.logging.log4j.core.LoggerContext ctx = (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
                ctx.updateLoggers(); // This causes all Loggers to refetch information from their LoggerConfig.
            } else {
                throw new RuntimeException(LOGGER_TYPE_UNKNOWN);
            }
        }
        return "success";
    }
    
    @Override
    public String setLogLevel(String logLevel) {
        return PARAMETER_TYPE_ERROR;
    }
    
}
