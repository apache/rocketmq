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
package org.apache.rocketmq.common.message;

import java.util.HashSet;

public class MessageConst {
    public static final String PROPERTY_KEYS = "KEYS";
    public static final String PROPERTY_TAGS = "TAGS";
    public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
    public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";
    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
    public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
    public static final String PROPERTY_EXTEND_UNIQ_INFO = "EXTEND_UNIQ_INFO";
    public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
    public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";
    public static final String PROPERTY_INNER_NUM = "INNER_NUM";
    public static final String PROPERTY_INNER_BASE = "INNER_BASE";
    public static final String DUP_INFO = "DUP_INFO";
    public static final String PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    public static final String PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
    public static final String PROPERTY_TRANSACTION_ID = "__transactionId__";
    public static final String PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";
    public static final String PROPERTY_INSTANCE_ID = "INSTANCE_ID";
    public static final String PROPERTY_CORRELATION_ID = "CORRELATION_ID";
    public static final String PROPERTY_MESSAGE_REPLY_TO_CLIENT = "REPLY_TO_CLIENT";
    public static final String PROPERTY_MESSAGE_TTL = "TTL";
    public static final String PROPERTY_REPLY_MESSAGE_ARRIVE_TIME = "ARRIVE_TIME";
    public static final String PROPERTY_PUSH_REPLY_TIME = "PUSH_REPLY_TIME";
    public static final String PROPERTY_CLUSTER = "CLUSTER";
    public static final String PROPERTY_MESSAGE_TYPE = "MSG_TYPE";
    public static final String PROPERTY_POP_CK = "POP_CK";
    public static final String PROPERTY_POP_CK_OFFSET = "POP_CK_OFFSET";
    public static final String PROPERTY_FIRST_POP_TIME = "1ST_POP_TIME";
    public static final String PROPERTY_SHARDING_KEY = "__SHARDINGKEY";
    public static final String PROPERTY_FORWARD_QUEUE_ID = "PROPERTY_FORWARD_QUEUE_ID";
    public static final String PROPERTY_REDIRECT = "REDIRECT";
    public static final String PROPERTY_INNER_MULTI_DISPATCH = "INNER_MULTI_DISPATCH";
    public static final String PROPERTY_INNER_MULTI_QUEUE_OFFSET = "INNER_MULTI_QUEUE_OFFSET";
    public static final String PROPERTY_TRACE_CONTEXT = "TRACE_CONTEXT";
    public static final String PROPERTY_TIMER_DELAY_SEC = "TIMER_DELAY_SEC";
    public static final String PROPERTY_TIMER_DELIVER_MS = "TIMER_DELIVER_MS";
    public static final String PROPERTY_BORN_HOST = "__BORNHOST";
    public static final String PROPERTY_BORN_TIMESTAMP = "BORN_TIMESTAMP";

    /**
     * property which name starts with "__RMQ.TRANSIENT." is called transient one that will not stored in broker disks.
     */
    public static final String PROPERTY_TRANSIENT_PREFIX = "__RMQ.TRANSIENT.";

    /**
     * the transient property key of topicSysFlag (set by client when pulling messages)
     */
    public static final String PROPERTY_TRANSIENT_TOPIC_CONFIG = PROPERTY_TRANSIENT_PREFIX + "TOPIC_SYS_FLAG";

    /**
     * the transient property key of groupSysFlag (set by client when pulling messages)
     */
    public static final String PROPERTY_TRANSIENT_GROUP_CONFIG = PROPERTY_TRANSIENT_PREFIX + "GROUP_SYS_FLAG";

    public static final String KEY_SEPARATOR = " ";

    public static final HashSet<String> STRING_HASH_SET = new HashSet<>(64);

    public static final String PROPERTY_TIMER_ENQUEUE_MS = "TIMER_ENQUEUE_MS";
    public static final String PROPERTY_TIMER_DEQUEUE_MS = "TIMER_DEQUEUE_MS";
    public static final String PROPERTY_TIMER_ROLL_TIMES = "TIMER_ROLL_TIMES";
    public static final String PROPERTY_TIMER_OUT_MS = "TIMER_OUT_MS";
    public static final String PROPERTY_TIMER_DEL_UNIQKEY = "TIMER_DEL_UNIQKEY";
    public static final String PROPERTY_TIMER_DEL_MS = "TIMER_DEL_MS";
    public static final String PROPERTY_TIMER_DELAY_LEVEL = "TIMER_DELAY_LEVEL";
    public static final String PROPERTY_TIMER_DELAY_MS = "TIMER_DELAY_MS";
    public static final String PROPERTY_CRC32 = "__CRC32#";

    /**
     * properties for DLQ
     */
    public static final String PROPERTY_DLQ_ORIGIN_TOPIC = "DLQ_ORIGIN_TOPIC";
    public static final String PROPERTY_DLQ_ORIGIN_MESSAGE_ID = "DLQ_ORIGIN_MESSAGE_ID";

    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
        STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
        STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
        STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
        STRING_HASH_SET.add(PROPERTY_POP_CK);
        STRING_HASH_SET.add(PROPERTY_POP_CK_OFFSET);
        STRING_HASH_SET.add(PROPERTY_FIRST_POP_TIME);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        STRING_HASH_SET.add(DUP_INFO);
        STRING_HASH_SET.add(PROPERTY_EXTEND_UNIQ_INFO);
        STRING_HASH_SET.add(PROPERTY_INSTANCE_ID);
        STRING_HASH_SET.add(PROPERTY_CORRELATION_ID);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_TTL);
        STRING_HASH_SET.add(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME);
        STRING_HASH_SET.add(PROPERTY_PUSH_REPLY_TIME);
        STRING_HASH_SET.add(PROPERTY_CLUSTER);
        STRING_HASH_SET.add(PROPERTY_MESSAGE_TYPE);
        STRING_HASH_SET.add(PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_SEC);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELIVER_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_ENQUEUE_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DEQUEUE_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_ROLL_TIMES);
        STRING_HASH_SET.add(PROPERTY_TIMER_OUT_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DEL_UNIQKEY);
        STRING_HASH_SET.add(PROPERTY_TIMER_DEL_MS);
        STRING_HASH_SET.add(PROPERTY_TIMER_DELAY_LEVEL);
        STRING_HASH_SET.add(PROPERTY_BORN_HOST);
        STRING_HASH_SET.add(PROPERTY_BORN_TIMESTAMP);
        STRING_HASH_SET.add(PROPERTY_DLQ_ORIGIN_TOPIC);
        STRING_HASH_SET.add(PROPERTY_DLQ_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_CRC32);
    }
}
