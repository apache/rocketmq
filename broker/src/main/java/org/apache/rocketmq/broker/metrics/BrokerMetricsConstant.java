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
package org.apache.rocketmq.broker.metrics;

public class BrokerMetricsConstant {
    public static final String OPEN_TELEMETRY_METER_NAME = "broker-meter";

    public static final String GAUGE_PROCESSOR_WATERMARK = "rocketmq_processor_watermark";
    public static final String GAUGE_BROKER_PERMISSION = "rocketmq_broker_permission";
    public static final String GAUGE_ACTIVE_TOPIC_NUM = "rocketmq_active_topic_number";
    public static final String GAUGE_ACTIVE_SUBGROUP_NUM = "rocketmq_active_subscription_number";

    public static final String COUNTER_MESSAGES_IN_TOTAL = "rocketmq_messages_in_total";
    public static final String COUNTER_MESSAGES_OUT_TOTAL = "rocketmq_messages_out_total";
    public static final String COUNTER_THROUGHPUT_IN_TOTAL = "rocketmq_throughput_in_total";
    public static final String COUNTER_THROUGHPUT_OUT_TOTAL = "rocketmq_throughput_out_total";
    public static final String HISTOGRAM_MESSAGE_SIZE = "rocketmq_message_size";
    public static final String HISTOGRAM_CREATE_TOPIC_TIME = "rocketmq_create_topic_time";
    public static final String HISTOGRAM_CREATE_SUBSCRIPTION_TIME = "rocketmq_create_subscription_time";

    public static final String GAUGE_PRODUCER_CONNECTIONS = "rocketmq_producer_connections";
    public static final String GAUGE_CONSUMER_CONNECTIONS = "rocketmq_consumer_connections";

    public static final String GAUGE_CONSUMER_LAG_MESSAGES = "rocketmq_consumer_lag_messages";
    public static final String GAUGE_CONSUMER_LAG_LATENCY = "rocketmq_consumer_lag_latency";
    public static final String GAUGE_CONSUMER_INFLIGHT_MESSAGES = "rocketmq_consumer_inflight_messages";
    public static final String GAUGE_CONSUMER_QUEUEING_LATENCY = "rocketmq_consumer_queueing_latency";
    public static final String GAUGE_CONSUMER_READY_MESSAGES = "rocketmq_consumer_ready_messages";
    public static final String COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL = "rocketmq_send_to_dlq_messages_total";

    public static final String COUNTER_COMMIT_MESSAGES_TOTAL = "rocketmq_commit_messages_total";
    public static final String COUNTER_ROLLBACK_MESSAGES_TOTAL = "rocketmq_rollback_messages_total";
    public static final String HISTOGRAM_FINISH_MSG_LATENCY = "rocketmq_finish_message_latency";
    public static final String GAUGE_HALF_MESSAGES = "rocketmq_half_messages";

    public static final String LABEL_CLUSTER_NAME = "cluster";
    public static final String LABEL_NODE_TYPE = "node_type";
    public static final String NODE_TYPE_BROKER = "broker";
    public static final String LABEL_NODE_ID = "node_id";
    public static final String LABEL_AGGREGATION = "aggregation";
    public static final String AGGREGATION_DELTA = "delta";
    public static final String LABEL_PROCESSOR = "processor";

    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_IS_RETRY = "is_retry";
    public static final String LABEL_REQUEST_IS_SUCCESS = "request_is_success";
    public static final String LABEL_IS_SYSTEM = "is_system";
    public static final String LABEL_CONSUMER_GROUP = "consumer_group";
    public static final String LABEL_MESSAGE_TYPE = "message_type";
    public static final String LABEL_LANGUAGE = "language";
    public static final String LABEL_VERSION = "version";
    public static final String LABEL_CONSUME_MODE = "consume_mode";
}
