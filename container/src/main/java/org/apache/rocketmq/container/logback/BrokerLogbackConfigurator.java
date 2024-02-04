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

package org.apache.rocketmq.container.logback;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class BrokerLogbackConfigurator {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final Set<String> CONFIGURED_BROKER_LIST = new HashSet<>();

    public static final String ROCKETMQ_LOGS = "rocketmqlogs";
    public static final String ROCKETMQ_PREFIX = "Rocketmq";
    public static final String SUFFIX_CONSOLE = "Console";
    public static final String SUFFIX_APPENDER = "Appender";
    public static final String SUFFIX_INNER_APPENDER = "_inner";

    public static void doConfigure(BrokerIdentity brokerIdentity) {
    }
}
