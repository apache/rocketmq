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

package org.apache.rocketmq.common.lite;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;

public class LiteUtil {

    public static final char SEPARATOR = '$';
    public static final String LITE_TOPIC_PREFIX = MixAll.LMQ_PREFIX + SEPARATOR;

    /**
     * Lite Topic: A specific type of message topic implemented based on LMQ, which has no retry topic.
     * A lite topic's underlying storage is a lmq (Light Message Queue),
     * but the reverse is not true: lmq is not necessarily a lite topic,
     * we use "$" as a separator to achieve the distinction and assume "$" is not allowed for topic name.
     * pattern like: %LMQ%$parentTopic$liteTopic
     *
     * @param parentTopic act as namespace
     * @param liteTopic here means child topic string
     * @return lmqName
     */
    public static String toLmqName(String parentTopic, String liteTopic) {
        if (StringUtils.isEmpty(parentTopic) || StringUtils.isEmpty(liteTopic)) {
            return null;
        }
        return LITE_TOPIC_PREFIX + parentTopic + SEPARATOR + liteTopic;
    }

    /**
     * whether lmqName is queue of a lite topic, here we only check the prefix.
     * @param lmqName
     * @return
     */
    public static boolean isLiteTopicQueue(String lmqName) {
        return lmqName != null && lmqName.startsWith(LITE_TOPIC_PREFIX);
    }

    public static String getParentTopic(String lmqName) {
        if (!isLiteTopicQueue(lmqName)) {
            return null;
        }
        int index = lmqName.indexOf(SEPARATOR, LITE_TOPIC_PREFIX.length());
        if (index == -1 || index == lmqName.length() - 1 || index == LITE_TOPIC_PREFIX.length()) {
            return null;
        }
        if (lmqName.indexOf(SEPARATOR, index + 1) != -1) {
            return null;
        }
        return lmqName.substring(LITE_TOPIC_PREFIX.length(), index);
    }

    public static String getLiteTopic(String lmqName) {
        if (!isLiteTopicQueue(lmqName)) {
            return null;
        }
        int index = lmqName.indexOf(SEPARATOR, LITE_TOPIC_PREFIX.length());
        if (index == -1 || index == lmqName.length() - 1 || index == LITE_TOPIC_PREFIX.length()) {
            return null;
        }
        if (lmqName.indexOf(SEPARATOR, index + 1) != -1) {
            return null;
        }
        return lmqName.substring(index + 1);
    }

    /**
     * %LMQ%${parentTopic}${liteTopic}
     * parse parent topic and child topic from lmqName
     * @param lmqName
     * @return
     */
    public static Pair<String, String> getParentAndLiteTopic(String lmqName) {
        if (null == lmqName || !lmqName.startsWith(LITE_TOPIC_PREFIX)) {
            return null;
        }
        String[] array = StringUtils.split(lmqName, SEPARATOR);
        if (array.length != 3) {
            return null;
        }
        return new Pair<>(array[1], array[2]);
    }

    /**
     * whether lmqName is queue of a lite topic and belongs to the specified parent,
     * here we only check the prefix.
     * @param lmqName
     * @param parentTopic
     * @return
     */
    public static boolean belongsTo(String lmqName, String parentTopic) {
        return lmqName != null && lmqName.startsWith(LITE_TOPIC_PREFIX + parentTopic + SEPARATOR);
    }
}
