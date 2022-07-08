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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.topic.TopicValidator;

public class PopAckConstants {
    public static long ackTimeInterval = 1000;
    public static final long SECOND = 1000;

    public static long lockTime = 5000;
    public static int retryQueueNum = 1;

    public static final String REVIVE_GROUP = MixAll.CID_RMQ_SYS_PREFIX + "REVIVE_GROUP";
    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String REVIVE_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "REVIVE_LOG_";
    public static final String CK_TAG = "ck";
    public static final String ACK_TAG = "ack";
    public static final String SPLIT = "@";

    /**
     * Build cluster revive topic
     *
     * @param clusterName cluster name
     * @return revive topic
     */
    public static String buildClusterReviveTopic(String clusterName) {
        return PopAckConstants.REVIVE_TOPIC + clusterName;
    }
}
