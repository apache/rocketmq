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
package org.apache.rocketmq.tieredstore.util;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class MessageStoreUtil {

    public static final String TIERED_STORE_LOGGER_NAME = "RocketmqTieredStore";
    public static final String RMQ_SYS_TIERED_STORE_INDEX_TOPIC = "rmq_sys_INDEX";

    public static final long BYTE = 1L;
    public static final long KB = BYTE << 10;
    public static final long MB = KB << 10;
    public static final long GB = MB << 10;
    public static final long TB = GB << 10;
    public static final long PB = TB << 10;
    public static final long EB = PB << 10;

    private static final DecimalFormat DEC_FORMAT = new DecimalFormat("#.##");

    private static String formatSize(long size, long divider, String unitName) {
        return DEC_FORMAT.format((double) size / divider) + unitName;
    }

    public static String toHumanReadable(long size) {
        if (size < 0)
            return String.valueOf(size);
        if (size >= EB)
            return formatSize(size, EB, "EB");
        if (size >= PB)
            return formatSize(size, PB, "PB");
        if (size >= TB)
            return formatSize(size, TB, "TB");
        if (size >= GB)
            return formatSize(size, GB, "GB");
        if (size >= MB)
            return formatSize(size, MB, "MB");
        if (size >= KB)
            return formatSize(size, KB, "KB");
        return formatSize(size, BYTE, "B");
    }

    public static String getHash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            return String.format("%032x", new BigInteger(1, digest)).substring(0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toFilePath(MessageQueue mq) {
        return String.format("%s/%s/%s", mq.getBrokerName(), mq.getTopic(), mq.getQueueId());
    }

    public static String getIndexFilePath(String brokerName) {
        return toFilePath(new MessageQueue(RMQ_SYS_TIERED_STORE_INDEX_TOPIC, brokerName, 0));
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(20);
        numberFormat.setMaximumFractionDigits(0);
        numberFormat.setGroupingUsed(false);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(Long.toString(offset).getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            String hash = String.format("%032x", new BigInteger(1, digest)).substring(0, 8);
            return hash + numberFormat.format(offset);
        } catch (Exception ignore) {
            return numberFormat.format(offset);
        }
    }

    public static long fileName2Offset(final String fileName) {
        return Long.parseLong(fileName.substring(fileName.length() - 20));
    }

    public static boolean isSlave(MessageStoreConfig storeConfig) {
        return storeConfig.getBrokerRole().equals(BrokerRole.SLAVE) ||
            storeConfig.isEnableDLegerCommitLog() && storeConfig.getBrokerRole().equals(BrokerRole.ASYNC_MASTER);
    }

    public static boolean isMaster(MessageStoreConfig storeConfig) {
        return !isSlave(storeConfig);
    }
}
