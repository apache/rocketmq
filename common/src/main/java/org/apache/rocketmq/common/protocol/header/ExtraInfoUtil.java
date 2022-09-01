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
package org.apache.rocketmq.common.protocol.header;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;

public class ExtraInfoUtil {
    private static final String NORMAL_TOPIC = "0";
    private static final String RETRY_TOPIC = "1";

    public static String[] split(String extraInfo) {
        if (extraInfo == null) {
            throw new IllegalArgumentException("split extraInfo is null");
        }
        return extraInfo.split(MessageConst.KEY_SEPARATOR);
    }

    public static Long getCkQueueOffset(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 1) {
            throw new IllegalArgumentException("getCkQueueOffset fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.valueOf(extraInfoStrs[0]);
    }

    public static Long getPopTime(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 2) {
            throw new IllegalArgumentException("getPopTime fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.valueOf(extraInfoStrs[1]);
    }

    public static Long getInvisibleTime(String[] extraInfoStrs) {
        if (extraInfoStrs == null || extraInfoStrs.length < 3) {
            throw new IllegalArgumentException("getInvisibleTime fail, extraInfoStrs length " + (extraInfoStrs == null ? 0 : extraInfoStrs.length));
        }
        return Long.valueOf(extraInfoStrs[2]);
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
            return KeyBuilder.buildPopRetryTopic(topic, cid);
        } else {
            return topic;
        }
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
        String t = NORMAL_TOPIC;
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            t = RETRY_TOPIC;
        }
        return ckQueueOffset + MessageConst.KEY_SEPARATOR + popTime + MessageConst.KEY_SEPARATOR + invisibleTime + MessageConst.KEY_SEPARATOR + reviveQid + MessageConst.KEY_SEPARATOR + t
            + MessageConst.KEY_SEPARATOR + brokerName + MessageConst.KEY_SEPARATOR + queueId;
    }

    public static String buildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime, int reviveQid, String topic, String brokerName, int queueId,
                                        long msgQueueOffset) {
        String t = NORMAL_TOPIC;
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            t = RETRY_TOPIC;
        }
        return ckQueueOffset
            + MessageConst.KEY_SEPARATOR + popTime + MessageConst.KEY_SEPARATOR + invisibleTime
            + MessageConst.KEY_SEPARATOR + reviveQid + MessageConst.KEY_SEPARATOR + t
            + MessageConst.KEY_SEPARATOR + brokerName + MessageConst.KEY_SEPARATOR + queueId
            + MessageConst.KEY_SEPARATOR + msgQueueOffset;
    }

    public static void buildStartOffsetInfo(StringBuilder stringBuilder, boolean retry, int queueId, long startOffset) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(retry ? RETRY_TOPIC : NORMAL_TOPIC)
            .append(MessageConst.KEY_SEPARATOR).append(queueId)
            .append(MessageConst.KEY_SEPARATOR).append(startOffset);
    }

    public static void buildOrderCountInfo(StringBuilder stringBuilder, boolean retry, int queueId, int orderCount) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(retry ? RETRY_TOPIC : NORMAL_TOPIC)
                .append(MessageConst.KEY_SEPARATOR).append(queueId)
                .append(MessageConst.KEY_SEPARATOR).append(orderCount);
    }

    public static void buildMsgOffsetInfo(StringBuilder stringBuilder, boolean retry, int queueId, List<Long> msgOffsets) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder(64);
        }

        if (stringBuilder.length() > 0) {
            stringBuilder.append(";");
        }

        stringBuilder.append(retry ? RETRY_TOPIC : NORMAL_TOPIC)
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

        Map<String, List<Long>> msgOffsetMap = new HashMap<String, List<Long>>(4);
        String[] array;
        if (msgOffsetInfo.indexOf(";") < 0) {
            array = new String[]{msgOffsetInfo};
        } else {
            array = msgOffsetInfo.split(";");
        }

        for (String one : array) {
            String[] split = one.split(MessageConst.KEY_SEPARATOR);
            if (split.length != 3) {
                throw new IllegalArgumentException("parse msgOffsetMap error, " + msgOffsetMap);
            }
            String key = split[0] + "@" + split[1];
            if (msgOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse msgOffsetMap error, duplicate, " + msgOffsetMap);
            }
            msgOffsetMap.put(key, new ArrayList<Long>(8));
            String[] msgOffsets = split[2].split(",");
            for (String msgOffset : msgOffsets) {
                msgOffsetMap.get(key).add(Long.valueOf(msgOffset));
            }
        }

        return msgOffsetMap;
    }

    public static Map<String, Long> parseStartOffsetInfo(String startOffsetInfo) {
        if (startOffsetInfo == null || startOffsetInfo.length() == 0) {
            return null;
        }
        Map<String, Long> startOffsetMap = new HashMap<String, Long>(4);
        String[] array;
        if (startOffsetInfo.indexOf(";") < 0) {
            array = new String[]{startOffsetInfo};
        } else {
            array = startOffsetInfo.split(";");
        }

        for (String one : array) {
            String[] split = one.split(MessageConst.KEY_SEPARATOR);
            if (split.length != 3) {
                throw new IllegalArgumentException("parse startOffsetInfo error, " + startOffsetInfo);
            }
            String key = split[0] + "@" + split[1];
            if (startOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse startOffsetInfo error, duplicate, " + startOffsetInfo);
            }
            startOffsetMap.put(key, Long.valueOf(split[2]));
        }

        return startOffsetMap;
    }

    public static Map<String, Integer> parseOrderCountInfo(String orderCountInfo) {
        if (orderCountInfo == null || orderCountInfo.length() == 0) {
            return null;
        }
        Map<String, Integer> startOffsetMap = new HashMap<String, Integer>(4);
        String[] array;
        if (orderCountInfo.indexOf(";") < 0) {
            array = new String[]{orderCountInfo};
        } else {
            array = orderCountInfo.split(";");
        }

        for (String one : array) {
            String[] split = one.split(MessageConst.KEY_SEPARATOR);
            if (split.length != 3) {
                throw new IllegalArgumentException("parse orderCountInfo error, " + orderCountInfo);
            }
            String key = split[0] + "@" + split[1];
            if (startOffsetMap.containsKey(key)) {
                throw new IllegalArgumentException("parse orderCountInfo error, duplicate, " + orderCountInfo);
            }
            startOffsetMap.put(key, Integer.valueOf(split[2]));
        }

        return startOffsetMap;
    }

    public static String getStartOffsetInfoMapKey(String topic, int queueId) {
        return (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) ? RETRY_TOPIC : NORMAL_TOPIC) + "@" + queueId;
    }
}
