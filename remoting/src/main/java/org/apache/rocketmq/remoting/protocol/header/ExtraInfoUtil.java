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
package org.apache.rocketmq.remoting.protocol.header;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;

public class ExtraInfoUtil {
    private static final String NORMAL_TOPIC = "0";
    private static final String RETRY_TOPIC = "1";
    private static final String RETRY_TOPIC_V2 = "2";
    private static final String QUEUE_OFFSET = "qo";

    public static String[] split(String extraInfo) {
        if (extraInfo == null) {
            throw new IllegalArgumentException("split extraInfo is null");
        }
        return extraInfo.split(MessageConst.KEY_SEPARATOR);
    }

    public static long getCkQueueOffset(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 1) {
            throw new IllegalArgumentException("getCkQueueOffset fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.parseLong(extraInfoStrs[0]);
    }

    public static long getPopTime(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 2) {
            throw new IllegalArgumentException("getPopTime fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.parseLong(extraInfoStrs[1]);
    }

    public static long getInvisibleTime(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 3) {
            throw new IllegalArgumentException("getInvisibleTime fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.parseLong(extraInfoStrs[2]);
    }

    public static int getReviveQid(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 4) {
            throw new IllegalArgumentException("getReviveQid fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Integer.parseInt(extraInfoStrs[3]);
    }

    public static String getRealTopic(String[] extraInfoStrs, String topic, String cid) {
        if (extraInfoStrs == null || extraInfoStrs.length < 5) {
            throw new IllegalArgumentException("getRealTopic fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        if (RETRY_TOPIC.equals(extraInfoStrs[4])) {
            return KeyBuilder.buildPopRetryTopicV1(topic, cid);
        } else if (RETRY_TOPIC_V2.equals(extraInfoStrs[4])) {
            return KeyBuilder.buildPopRetryTopicV2(topic, cid);
        } else {
            return topic;
        }
    }

    public static String getRealTopic(String topic, String cid, String retry) {
        if (retry.equals(NORMAL_TOPIC)) {
            return topic;
        } else if (retry.equals(RETRY_TOPIC)) {
            return KeyBuilder.buildPopRetryTopicV1(topic, cid);
        } else if (retry.equals(RETRY_TOPIC_V2)) {
            return KeyBuilder.buildPopRetryTopicV2(topic, cid);
        } else {
            throw new IllegalArgumentException("getRetry fail, format is wrong");
        }
    }

    public static String getRetry(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 5) {
            throw new IllegalArgumentException("getRetry fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return extraInfoStrs[4];
    }

    public static String getBrokerName(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 6) {
            throw new IllegalArgumentException("getBrokerName fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return extraInfoStrs[5];
    }

    public static int getQueueId(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 7) {
            throw new IllegalArgumentException("getQueueId fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Integer.parseInt(extraInfoStrs[6]);
    }

    public static long getQueueOffset(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 8) {
            throw new IllegalArgumentException("getQueueOffset fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.parseLong(extraInfoStrs[7]);
    }

    public static String buildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid, String topic, String brokerName, int queueId) {
        String t = getRetry(topic);
        return ckQueueOffset + MessageConst.KEY_SEPARATOR + popTime + MessageConst.KEY_SEPARATOR + invisibleTime + MessageConst.KEY_SEPARATOR + reviveQid + MessageConst.KEY_SEPARATOR + t
            + MessageConst.KEY_SEPARATOR + brokerName + MessageConst.KEY_SEPARATOR + queueId;
    }

    public static String buildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid, String topic, String brokerName, int queueId,
                                        long msgQueueOffset) {
        String t = getRetry(topic);
        return ckQueueOffset
            + MessageConst.KEY_SEPARATOR + popTime + MessageConst.KEY_SEPARATOR + invisibleTime
            + MessageConst.KEY_SEPARATOR + reviveQid + MessageConst.KEY_SEPARATOR + t
            + MessageConst.KEY_SEPARATOR + brokerName + MessageConst.KEY_SEPARATOR + queueId
            + MessageConst.KEY_SEPARATOR + msgQueueOffset;
    }

    public static void buildStartOffsetInfo(StringBuilder stringBuilder, String topic, int queueId, long startOffset) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(getRetry(topic))
            .append(MessageConst.KEY_SEPARATOR).append(queueId)
            .append(MessageConst.KEY_SEPARATOR).append(startOffset);
    }

    public static void buildQueueIdOrderCountInfo(StringBuilder stringBuilder, String topic, int queueId, int orderCount) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(getRetry(topic))
                .append(MessageConst.KEY_SEPARATOR).append(queueId)
                .append(MessageConst.KEY_SEPARATOR).append(orderCount);
    }

    public static void buildQueueOffsetOrderCountInfo(StringBuilder stringBuilder, String topic, long queueId, long queueOffset, int orderCount) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(getRetry(topic))
            .append(MessageConst.KEY_SEPARATOR).append(getQueueOffsetKeyValueKey(queueId, queueOffset))
            .append(MessageConst.KEY_SEPARATOR).append(orderCount);
    }

    public static void buildMsgOffsetInfo(StringBuilder stringBuilder, String topic, int queueId, List<Long> msgOffsets) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(getRetry(topic))
            .append(MessageConst.KEY_SEPARATOR).append(queueId)
            .append(MessageConst.KEY_SEPARATOR);

        for (int i = 0; i < msgOffsets.size(); i++) {
            stringBuilder.append(msgOffsets.get(i));
            if (i < msgOffsets.size() - 1) {
                stringBuilder.append(",");
            }
        }
    }

    public static Map<String, List<Long>> parseMsgOffsetInfo(String msgOffsetInfo) {
        if (msgOffsetInfo == null || msgOffsetInfo.length() == 0) {
            return null;
        }

        Map<String, List<Long>> msgOffsetMap = new HashMap<>(4);
        String[] array;
        if (msgOffsetInfo.indexOf(";") < 0) {
            array = new String[]{msgOffsetInfo};
        } else {
            array = msgOffsetInfo.split(";");
        }

        for (String one : array) {
            long separators = locateEntrySeparators(one);
            if (separators < 0) {
                throw new IllegalArgumentException("parse msgOffsetInfo error, " + msgOffsetInfo);
            }
            int sep1 = (int) (separators >>> 32);
            int sep2 = (int) separators;
            String key = buildEntryKey(one, sep1, sep2);
            if (msgOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse msgOffsetInfo error, duplicate, " + msgOffsetInfo);
            }
            List<Long> msgOffsets = new ArrayList<>(8);
            msgOffsetMap.put(key, msgOffsets);
            int start = sep2 + 1;
            while (start < one.length()) {
                int comma = one.indexOf(',', start);
                int end = comma < 0 ? one.length() : comma;
                msgOffsets.add(Long.valueOf(one.substring(start, end)));
                if (comma < 0) {
                    break;
                }
                start = comma + 1;
            }
        }

        return msgOffsetMap;
    }

    public static Map<String, Long> parseStartOffsetInfo(String startOffsetInfo) {
        if (startOffsetInfo == null || startOffsetInfo.length() == 0) {
            return null;
        }
        Map<String, Long> startOffsetMap = new HashMap<>(4);
        String[] array;
        if (startOffsetInfo.indexOf(";") < 0) {
            array = new String[]{startOffsetInfo};
        } else {
            array = startOffsetInfo.split(";");
        }

        for (String one : array) {
            long separators = locateEntrySeparators(one);
            if (separators < 0) {
                throw new IllegalArgumentException("parse startOffsetInfo error, " + startOffsetInfo);
            }
            int sep1 = (int) (separators >>> 32);
            int sep2 = (int) separators;
            String key = buildEntryKey(one, sep1, sep2);
            if (startOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse startOffsetInfo error, duplicate, " + startOffsetInfo);
            }
            startOffsetMap.put(key, Long.valueOf(one.substring(sep2 + 1)));
        }

        return startOffsetMap;
    }

    public static Map<String, Integer> parseOrderCountInfo(String orderCountInfo) {
        if (orderCountInfo == null || orderCountInfo.length() == 0) {
            return null;
        }
        Map<String, Integer> startOffsetMap = new HashMap<>(4);
        String[] array;
        if (orderCountInfo.indexOf(";") < 0) {
            array = new String[]{orderCountInfo};
        } else {
            array = orderCountInfo.split(";");
        }

        for (String one : array) {
            long separators = locateEntrySeparators(one);
            if (separators < 0) {
                throw new IllegalArgumentException("parse orderCountInfo error, " + orderCountInfo);
            }
            int sep1 = (int) (separators >>> 32);
            int sep2 = (int) separators;
            String key = buildEntryKey(one, sep1, sep2);
            if (startOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse orderCountInfo error, duplicate, " + orderCountInfo);
            }
            startOffsetMap.put(key, Integer.valueOf(one.substring(sep2 + 1)));
        }

        return startOffsetMap;
    }

    /**
     * Locates the two {@link MessageConst#KEY_SEPARATOR} positions of an entry laid out as
     * {@code retryFlag queueId value}, packed as {@code (sep1 << 32) | sep2}. Returns a negative
     * value when the entry does not have exactly three non-empty fields. This is stricter than
     * the previous split-based validation, which half-accepted entries with empty fields or
     * trailing separators; such corrupt entries are now rejected with the same
     * {@code IllegalArgumentException} the callers already throw for other malformed shapes.
     */
    private static long locateEntrySeparators(String one) {
        int sep1 = one.indexOf(MessageConst.KEY_SEPARATOR);
        if (sep1 <= 0) {
            return -1;
        }
        int sep2 = one.indexOf(MessageConst.KEY_SEPARATOR, sep1 + 1);
        if (sep2 <= sep1 + 1 || sep2 >= one.length() - 1
            || one.indexOf(MessageConst.KEY_SEPARATOR, sep2 + 1) >= 0) {
            return -1;
        }
        return ((long) sep1 << 32) | sep2;
    }

    private static String buildEntryKey(String one, int sep1, int sep2) {
        return new StringBuilder(sep2 + 1)
            .append(one, 0, sep1).append('@').append(one, sep1 + 1, sep2).toString();
    }

    public static List<Integer> parseLiteOrderCountInfo(String orderCountInfo, int msgCount) {
        if (StringUtils.isEmpty(orderCountInfo)) {
            return null;
        }
        String[] infos = orderCountInfo.split(";");
        if (infos.length != msgCount) {
            return null;
        }
        return Arrays.stream(infos).map(ExtraInfoUtil::parseLiteOrderCount).collect(Collectors.toList());
    }

    private static int parseLiteOrderCount(String info) {
        if (StringUtils.isBlank(info)) {
            return 0;
        }
        if (!info.contains(QUEUE_OFFSET)) {
            return NumberUtils.toInt(info, 0);
        }
        String[] split = info.split(MessageConst.KEY_SEPARATOR);
        return split.length != 3 ? 0 : NumberUtils.toInt(split[2], 0);
    }

    public static String getStartOffsetInfoMapKey(String topic, long key) {
        return getRetry(topic) + "@" + key;
    }

    public static String getStartOffsetInfoMapKey(String topic, String popCk, long key) {
        return getRetry(topic, popCk) + "@" + key;
    }

    public static String getQueueOffsetKeyValueKey(long queueId, long queueOffset) {
        return QUEUE_OFFSET + queueId + "%" + queueOffset;
    }

    public static String getQueueOffsetMapKey(String topic, long queueId, long queueOffset) {
        return getRetry(topic) + "@" + getQueueOffsetKeyValueKey(queueId, queueOffset);
    }

    public static boolean isOrder(String[] extraInfo) {
        return ExtraInfoUtil.getReviveQid(extraInfo) == KeyBuilder.POP_ORDER_REVIVE_QUEUE;
    }

    private static String getRetry(String topic) {
        String t = NORMAL_TOPIC;
        if (KeyBuilder.isPopRetryTopicV2(topic)) {
            t = RETRY_TOPIC_V2;
        } else if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            t = RETRY_TOPIC;
        }
        return t;
    }

    private static String getRetry(String topic, String popCk) {
        if (popCk != null) {
            return getRetry(split(popCk));
        }
        return getRetry(topic);
    }
}
