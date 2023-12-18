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
    public static final String GAUGE_TIMER_MESSAGE_SNAPSHOT = "rocketmq_timer_message_snapshot";
    public static final String HISTOGRAM_DELAY_MSG_LATENCY = "rocketmq_delay_message_latency";

    public static final String LABEL_STORAGE_TYPE = "storage_type";
    public static final String DEFAULT_STORAGE_TYPE = "local";
    public static final String LABEL_STORAGE_MEDIUM = "storage_medium";
    public static final String DEFAULT_STORAGE_MEDIUM = "disk";
    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_TIMING_BOUND = "timer_bound_s";
    public static final String GAUGE_BYTES_ROCKSDB_WRITTEN = "rocketmq_rocksdb_bytes_written";
    public static final String GAUGE_BYTES_ROCKSDB_READ = "rocketmq_rocksdb_bytes_read";

    public static final String GAUGE_TIMES_ROCKSDB_WRITTEN_SELF = "rocketmq_rocksdb_times_written_self";
    public static final String GAUGE_TIMES_ROCKSDB_WRITTEN_OTHER = "rocketmq_rocksdb_times_written_other";
    public static final String GAUGE_RATE_ROCKSDB_CACHE_HIT = "rocketmq_rocksdb_rate_cache_hit";
}
