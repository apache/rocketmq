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

package org.apache.rocketmq.thinclient.metrics;

public enum MetricName {
    /**
     * A counter that records the number of successful api calls of message publishing.
     */
    SEND_SUCCESS_TOTAL("rocketmq_send_success_total"),
    /**
     * A counter that records the number of failed api calls of message publishing.
     */
    SEND_FAILURE_TOTAL("rocketmq_send_failure_total"),
    /**
     * A histogram that records the cost time of successful api calls of message publishing.
     */
    SEND_SUCCESS_COST_TIME("rocketmq_send_success_cost_time"),
    /**
     * A counter that records the number of successful consumption of message.
     */
    PROCESS_SUCCESS_TOTAL("rocketmq_process_success_total"),
    /**
     * A counter that records the number of failed consumption of message.
     */
    PROCESS_FAILURE_TOTAL("rocketmq_process_failure_total"),
    /**
     * A counter that records the process time of message consumption.
     */
    PROCESS_TIME("rocketmq_process_time"),
    /**
     * A counter that records the number of successful acknowledgement of message.
     */
    ACK_SUCCESS_TOTAL("rocketmq_ack_success_total"),
    /**
     * A counter that records the number of failed acknowledgement of message.
     */
    ACK_FAILURE_TOTAL("rocketmq_ack_failure_total"),
    /**
     * A counter that records the number of successful changing invisible duration of message.
     */
    CHANGE_INVISIBLE_DURATION_SUCCESS_TOTAL("rocketmq_change_invisible_duration_success_total"),
    /**
     * A counter that records the number of failed changing invisible duration of message.
     */
    CHANGE_INVISIBLE_DURATION_FAILURE_TOTAL("rocketmq_change_invisible_duration_failure_total"),
    /**
     * A gauge that records the cached message count of push consumer.
     */
    CONSUMER_CACHED_MESSAGES("rocketmq_consumer_cached_messages"),
    /**
     * A gauge that records the cached message bytes of push consumer.
     */
    CONSUMER_CACHED_BYTES("rocketmq_consumer_cached_bytes"),
    /**
     * A histogram that records the latency of message delivery from remote.
     */
    DELIVERY_LATENCY("rocketmq_delivery_latency"),
    /**
     * A histogram that records await time of message consumption.
     */
    AWAIT_TIME("rocketmq_await_time");

    private final String name;

    MetricName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
