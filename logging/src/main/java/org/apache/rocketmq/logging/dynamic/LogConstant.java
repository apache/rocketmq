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

/**
 * log constant
 */
public class LogConstant {
    public static final String LOG4J_LOGGER_FACTORY = "org.slf4j.impl.Log4jLoggerFactory";
    public static final String LOG4J2_LOGGER_FACTORY = "org.apache.logging.slf4j.Log4jLoggerFactory";
    public static final String LOGBACK_LOGGER_FACTORY = "ch.qos.logback.classic.util.ContextSelectorStaticBinder";
    
    public static final String ROOT_KEY = "root";
    public static final String LOGGER_NAME = "loggerName";
    public static final String LOGGER_LEVEL = "loggerLevel";
    public static final String LOGGER_TYPE_UNKNOWN = "The type of Logger is unknown and cannot be processed!";
    public static final String LOGGER_NOT_EXSIT = "The Logger that needs to modify the log level does not exist!";
    public static final String PARAMETER_TYPE_ERROR = "The parameter is of the wrong type and cannot be processed!";
    public static final String LOGGER_LIST_IS_NULL = "loggerList is empty and cannot be processed!";
    
    public static final String LOG_FRAMEWORK = "logFramework";
    public static final String LOGGER_LIST = "loggerList";
    
}
