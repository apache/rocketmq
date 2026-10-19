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
package org.apache.rocketmq.tools.command.message;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * Validated configuration for the push-consumer diagnostic command.
 */
public class PushConsumeMessageConfig {
    static final long DEFAULT_MAX_WAIT_SECONDS = 0;
    static final long DEFAULT_MAX_MESSAGES = 0;
    static final int DEFAULT_BATCH_SIZE = 1;

    private final String topic;
    private final String consumerGroup;
    private final String subExpression;
    private final String instanceName;
    private final long maxMessages;
    private final long maxWaitMillis;
    private final int batchSize;
    private final boolean orderly;
    private final boolean messageTraceEnabled;
    private final String customizedTraceTopic;
    private final boolean printBody;
    private final boolean printProperties;
    private final Charset charset;
    private final ConsumeFromWhere consumeFromWhere;
    private final String consumeTimestamp;

    PushConsumeMessageConfig(String topic, String consumerGroup, String subExpression, String instanceName,
        long maxMessages, long maxWaitMillis, int batchSize, boolean orderly, boolean messageTraceEnabled,
        String customizedTraceTopic, boolean printBody, boolean printProperties, Charset charset,
        ConsumeFromWhere consumeFromWhere, String consumeTimestamp) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.subExpression = subExpression;
        this.instanceName = instanceName;
        this.maxMessages = maxMessages;
        this.maxWaitMillis = maxWaitMillis;
        this.batchSize = batchSize;
        this.orderly = orderly;
        this.messageTraceEnabled = messageTraceEnabled;
        this.customizedTraceTopic = customizedTraceTopic;
        this.printBody = printBody;
        this.printProperties = printProperties;
        this.charset = charset;
        this.consumeFromWhere = consumeFromWhere;
        this.consumeTimestamp = consumeTimestamp;
    }

    public static PushConsumeMessageConfig fromCommandLine(CommandLine commandLine) {
        String topic = requiredValue(commandLine, 't', "topic");
        String consumerGroup = requiredValue(commandLine, 'g', "consumerGroup");
        String subExpression = optionalValue(commandLine, 's', "*");
        String instanceName = optionalValue(commandLine, 'i', buildDefaultInstanceName());
        long maxMessages = parseNonNegativeLong(commandLine, 'c', DEFAULT_MAX_MESSAGES, "messageCount");
        long maxWaitSeconds = parseNonNegativeLong(commandLine, 'w', DEFAULT_MAX_WAIT_SECONDS, "maxWaitSeconds");
        int batchSize = parseBatchSize(commandLine);
        boolean orderly = parseBoolean(commandLine, 'o', false, "orderly");
        boolean traceEnabled = parseBoolean(commandLine, 'm', false, "messageTraceEnabled");
        String traceTopic = optionalNullableValue(commandLine, 'r');
        boolean printBody = parseBoolean(commandLine, 'd', true, "printBody");
        boolean printProperties = parseBoolean(commandLine, 'p', false, "printProperties");
        Charset charset = parseCharset(commandLine);
        ConsumeFromWhere consumeFromWhere = parseConsumeFromWhere(commandLine);
        String consumeTimestamp = optionalNullableValue(commandLine, 'x');

        if (consumeFromWhere == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP && StringUtils.isBlank(consumeTimestamp)) {
            throw new IllegalArgumentException("consumeTimestamp is required when consumeFromWhere is TIMESTAMP");
        }
        if (consumeTimestamp != null && UtilAll.parseDate(consumeTimestamp, UtilAll.YYYYMMDDHHMMSS) == null) {
            throw new IllegalArgumentException("consumeTimestamp must use yyyyMMddHHmmss format: " + consumeTimestamp);
        }
        if (consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_TIMESTAMP && consumeTimestamp != null) {
            throw new IllegalArgumentException("consumeTimestamp can only be used with consumeFromWhere TIMESTAMP");
        }
        if (!traceEnabled && traceTopic != null) {
            throw new IllegalArgumentException("customizedTraceTopic requires messageTraceEnabled=true");
        }

        return new PushConsumeMessageConfig(topic, consumerGroup, subExpression, instanceName, maxMessages,
            secondsToMillis(maxWaitSeconds), batchSize, orderly, traceEnabled, traceTopic, printBody,
            printProperties, charset, consumeFromWhere, consumeTimestamp);
    }

    private static long secondsToMillis(long seconds) {
        try {
            return Math.multiplyExact(seconds, 1000);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("maxWaitSeconds is too large: " + seconds, e);
        }
    }

    private static String requiredValue(CommandLine commandLine, char option, String optionName) {
        String value = optionalNullableValue(commandLine, option);
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(optionName + " must not be blank");
        }
        return value;
    }

    private static String optionalValue(CommandLine commandLine, char option, String defaultValue) {
        String value = optionalNullableValue(commandLine, option);
        return value == null ? defaultValue : value;
    }

    private static String optionalNullableValue(CommandLine commandLine, char option) {
        if (!commandLine.hasOption(option)) {
            return null;
        }
        String value = commandLine.getOptionValue(option);
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value.isEmpty() ? null : value;
    }

    private static long parseNonNegativeLong(CommandLine commandLine, char option, long defaultValue,
        String optionName) {
        String value = optionalNullableValue(commandLine, option);
        if (value == null) {
            return defaultValue;
        }

        final long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be an integer: " + value, e);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(optionName + " must be greater than or equal to zero");
        }
        return parsed;
    }

    private static int parseBatchSize(CommandLine commandLine) {
        String value = optionalNullableValue(commandLine, 'b');
        if (value == null) {
            return DEFAULT_BATCH_SIZE;
        }

        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("batchSize must be an integer: " + value, e);
        }
        if (parsed < 1 || parsed > 1024) {
            throw new IllegalArgumentException("batchSize must be between 1 and 1024");
        }
        return parsed;
    }

    private static boolean parseBoolean(CommandLine commandLine, char option, boolean defaultValue,
        String optionName) {
        String value = optionalNullableValue(commandLine, option);
        if (value == null) {
            return defaultValue;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(optionName + " must be true or false: " + value);
        }
        return Boolean.parseBoolean(value);
    }

    private static Charset parseCharset(CommandLine commandLine) {
        String value = optionalNullableValue(commandLine, 'a');
        if (value == null) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unsupported charset: " + value, e);
        }
    }

    private static ConsumeFromWhere parseConsumeFromWhere(CommandLine commandLine) {
        String value = optionalValue(commandLine, 'f', "LAST").toUpperCase(Locale.ROOT);
        switch (value) {
            case "LAST":
                return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
            case "FIRST":
                return ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
            case "TIMESTAMP":
                return ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
            default:
                throw new IllegalArgumentException("consumeFromWhere must be LAST, FIRST, or TIMESTAMP: " + value);
        }
    }

    private static String buildDefaultInstanceName() {
        return "mqadmin_push_" + System.currentTimeMillis();
    }

    public String getTopic() {
        return topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getSubExpression() {
        return subExpression;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public long getMaxMessages() {
        return maxMessages;
    }

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isOrderly() {
        return orderly;
    }

    public boolean isMessageTraceEnabled() {
        return messageTraceEnabled;
    }

    public String getCustomizedTraceTopic() {
        return customizedTraceTopic;
    }

    public boolean isPrintBody() {
        return printBody;
    }

    public boolean isPrintProperties() {
        return printProperties;
    }

    public Charset getCharset() {
        return charset;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }
}
