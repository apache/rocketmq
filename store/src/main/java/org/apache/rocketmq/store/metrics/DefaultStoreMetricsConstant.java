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
package org.apache.rocketmq.store.metrics;

public class DefaultStoreMetricsConstant {
    public static final String GAUGE_STORAGE_SIZE = "rocketmq_storage_size";
    public static final String GAUGE_STORAGE_FLUSH_BEHIND = "rocketmq_storage_flush_behind_bytes";
    public static final String GAUGE_STORAGE_DISPATCH_BEHIND = "rocketmq_storage_dispatch_behind_bytes";
    public static final String GAUGE_STORAGE_MESSAGE_RESERVE_TIME = "rocketmq_storage_message_reserve_time";

    public static final String GAUGE_TIMER_ENQUEUE_LAG = "rocketmq_timer_enqueue_lag";
    public static final String GAUGE_TIMER_ENQUEUE_LATENCY = "rocketmq_timer_enqueue_latency";
    public static final String GAUGE_TIMER_DEQUEUE_LAG = "rocketmq_timer_dequeue_lag";
    public static final String GAUGE_TIMER_DEQUEUE_LATENCY = "rocketmq_timer_dequeue_latency";
    public static final String GAUGE_TIMING_MESSAGES = "rocketmq_timing_messages";

    public static final String COUNTER_TIMER_ENQUEUE_TOTAL = "rocketmq_timer_enqueue_total";
    public static final String COUNTER_TIMER_DEQUEUE_TOTAL = "rocketmq_timer_dequeue_total";

    public static final String LABEL_STORAGE_TYPE = "storage_type";
    public static final String DEFAULT_STORAGE_TYPE = "local";
    public static final String LABEL_STORAGE_MEDIUM = "storage_medium";
    public static final String DEFAULT_STORAGE_MEDIUM = "disk";
    public static final String LABEL_TOPIC = "topic";
}
