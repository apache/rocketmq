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

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.rolling.TimeBasedFileNamingAndTriggeringPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import ch.qos.logback.core.util.FileSize;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.slf4j.LoggerFactory;

public class BrokerLogbackConfigurator {
    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final Set<String> CONFIGURED_BROKER_LIST = new HashSet<>();

    public static final String ROCKETMQ_LOGS = "rocketmqlogs";
    public static final String ROCKETMQ_PREFIX = "Rocketmq";
    public static final String SUFFIX_CONSOLE = "Console";
    public static final String SUFFIX_APPENDER = "Appender";
    public static final String SUFFIX_INNER_APPENDER = "_inner";

    public static void doConfigure(BrokerIdentity brokerIdentity) {
        if (!CONFIGURED_BROKER_LIST.contains(brokerIdentity.getCanonicalName())) {
            try {
                LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
                for (ch.qos.logback.classic.Logger tempLogger : lc.getLoggerList()) {
                    String loggerName = tempLogger.getName();
                    if (loggerName.startsWith(ROCKETMQ_PREFIX)
                        && !loggerName.endsWith(SUFFIX_CONSOLE)
                        && !loggerName.equals(LoggerName.ACCOUNT_LOGGER_NAME)
                        && !loggerName.equals(LoggerName.COMMERCIAL_LOGGER_NAME)
                        && !loggerName.equals(LoggerName.CONSUMER_STATS_LOGGER_NAME)) {
                        ch.qos.logback.classic.Logger logger = lc.getLogger(brokerIdentity.getLoggerIdentifier() + loggerName);
                        logger.setAdditive(tempLogger.isAdditive());
                        logger.setLevel(tempLogger.getLevel());
                        String appenderName = loggerName + SUFFIX_APPENDER;
                        Appender<ILoggingEvent> tempAppender = tempLogger.getAppender(appenderName);
                        if (tempAppender instanceof AsyncAppender) {
                            AsyncAppender tempAsyncAppender = (AsyncAppender) tempAppender;
                            AsyncAppender asyncAppender = new AsyncAppender();
                            asyncAppender.setName(brokerIdentity.getLoggerIdentifier() + appenderName);
                            asyncAppender.setContext(tempAsyncAppender.getContext());

                            String innerAppenderName = appenderName + SUFFIX_INNER_APPENDER;
                            Appender<ILoggingEvent> tempInnerAppender = tempAsyncAppender.getAppender(innerAppenderName);
                            if (!(tempInnerAppender instanceof RollingFileAppender)) {
                                continue;
                            }
                            asyncAppender.addAppender(configureRollingFileAppender((RollingFileAppender<ILoggingEvent>) tempInnerAppender,
                                brokerIdentity, innerAppenderName));
                            asyncAppender.start();
                            logger.addAppender(asyncAppender);
                        } else if (tempAppender instanceof RollingFileAppender) {
                            logger.addAppender(configureRollingFileAppender((RollingFileAppender<ILoggingEvent>) tempAppender,
                                brokerIdentity, appenderName));
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Configure logback for broker {} failed, will use default broker log config instead. {}", brokerIdentity.getCanonicalName(), e);
                return;
            }

            CONFIGURED_BROKER_LIST.add(brokerIdentity.getCanonicalName());
        }
    }

    private static RollingFileAppender<ILoggingEvent> configureRollingFileAppender(
        RollingFileAppender<ILoggingEvent> tempRollingFileAppender, BrokerIdentity brokerIdentity, String appenderName)
        throws NoSuchFieldException, IllegalAccessException {
        RollingFileAppender<ILoggingEvent> rollingFileAppender = new RollingFileAppender<>();

        // configure appender name
        rollingFileAppender.setName(brokerIdentity.getLoggerIdentifier() + appenderName);

        // configure file name
        rollingFileAppender.setFile(tempRollingFileAppender.getFile().replaceAll(ROCKETMQ_LOGS, brokerIdentity.getCanonicalName() + "_" + ROCKETMQ_LOGS));

        // configure append
        rollingFileAppender.setAppend(true);

        // configure prudent
        rollingFileAppender.setPrudent(tempRollingFileAppender.isPrudent());

        // configure rollingPolicy
        RollingPolicy originalRollingPolicy = tempRollingFileAppender.getRollingPolicy();
        if (originalRollingPolicy instanceof TimeBasedRollingPolicy) {
            TimeBasedRollingPolicy<ILoggingEvent> tempRollingPolicy = (TimeBasedRollingPolicy<ILoggingEvent>) originalRollingPolicy;
            TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
            rollingPolicy.setContext(tempRollingPolicy.getContext());
            rollingPolicy.setFileNamePattern(tempRollingPolicy.getFileNamePattern());
            SizeAndTimeBasedFNATP<ILoggingEvent> sizeAndTimeBasedFNATP = new SizeAndTimeBasedFNATP<>();
            sizeAndTimeBasedFNATP.setContext(tempRollingPolicy.getContext());
            TimeBasedFileNamingAndTriggeringPolicy<ILoggingEvent> timeBasedFileNamingAndTriggeringPolicy =
                tempRollingPolicy.getTimeBasedFileNamingAndTriggeringPolicy();
            if (timeBasedFileNamingAndTriggeringPolicy instanceof SizeAndTimeBasedFNATP) {
                SizeAndTimeBasedFNATP<ILoggingEvent> originalSizeAndTimeBasedFNATP =
                    (SizeAndTimeBasedFNATP<ILoggingEvent>) timeBasedFileNamingAndTriggeringPolicy;
                Field field = originalSizeAndTimeBasedFNATP.getClass().getDeclaredField("maxFileSize");
                field.setAccessible(true);
                sizeAndTimeBasedFNATP.setMaxFileSize((FileSize) field.get(originalSizeAndTimeBasedFNATP));
                sizeAndTimeBasedFNATP.setTimeBasedRollingPolicy(rollingPolicy);
            }
            rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(sizeAndTimeBasedFNATP);
            rollingPolicy.setMaxHistory(tempRollingPolicy.getMaxHistory());
            rollingPolicy.setParent(rollingFileAppender);
            rollingPolicy.start();
            rollingFileAppender.setRollingPolicy(rollingPolicy);
        } else if (originalRollingPolicy instanceof FixedWindowRollingPolicy) {
            FixedWindowRollingPolicy tempRollingPolicy = (FixedWindowRollingPolicy) originalRollingPolicy;
            FixedWindowRollingPolicy rollingPolicy = new FixedWindowRollingPolicy();
            rollingPolicy.setContext(tempRollingPolicy.getContext());
            rollingPolicy.setFileNamePattern(tempRollingPolicy.getFileNamePattern().replaceAll(ROCKETMQ_LOGS, brokerIdentity.getCanonicalName() + "_" + ROCKETMQ_LOGS));
            rollingPolicy.setMaxIndex(tempRollingPolicy.getMaxIndex());
            rollingPolicy.setMinIndex(tempRollingPolicy.getMinIndex());
            rollingPolicy.setParent(rollingFileAppender);
            rollingPolicy.start();
            rollingFileAppender.setRollingPolicy(rollingPolicy);
        }

        // configure triggerPolicy
        if (tempRollingFileAppender.getTriggeringPolicy() instanceof SizeBasedTriggeringPolicy) {
            SizeBasedTriggeringPolicy<ILoggingEvent> tempTriggerPolicy = (SizeBasedTriggeringPolicy<ILoggingEvent>) tempRollingFileAppender.getTriggeringPolicy();
            SizeBasedTriggeringPolicy<ILoggingEvent> triggerPolicy = new SizeBasedTriggeringPolicy<>();
            triggerPolicy.setContext(tempTriggerPolicy.getContext());
            Field field = triggerPolicy.getClass().getDeclaredField("maxFileSize");
            field.setAccessible(true);
            triggerPolicy.setMaxFileSize((FileSize) field.get(triggerPolicy));
            triggerPolicy.start();
            rollingFileAppender.setTriggeringPolicy(triggerPolicy);
        }

        // configure encoder
        Encoder<ILoggingEvent> tempEncoder = tempRollingFileAppender.getEncoder();
        if (tempEncoder instanceof PatternLayoutEncoder) {
            PatternLayoutEncoder tempPatternLayoutEncoder = (PatternLayoutEncoder) tempEncoder;
            PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
            patternLayoutEncoder.setContext(tempPatternLayoutEncoder.getContext());
            patternLayoutEncoder.setPattern(tempPatternLayoutEncoder.getPattern());
            patternLayoutEncoder.setCharset(tempPatternLayoutEncoder.getCharset());
            patternLayoutEncoder.start();

            rollingFileAppender.setEncoder(patternLayoutEncoder);
        }

        // configure context
        rollingFileAppender.setContext(tempRollingFileAppender.getContext());

        rollingFileAppender.start();

        return rollingFileAppender;
    }
}
