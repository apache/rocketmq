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
package io.openmessaging.rocketmq.domain;

public interface NonStandardKeys {
    String CONSUMER_GROUP = "rmq.consumer.group";
    String PRODUCER_GROUP = "rmq.producer.group";
    String MAX_REDELIVERY_TIMES = "rmq.max.redelivery.times";
    String MESSAGE_CONSUME_TIMEOUT = "rmq.message.consume.timeout";
    String MAX_CONSUME_THREAD_NUMS = "rmq.max.consume.thread.nums";
    String MIN_CONSUME_THREAD_NUMS = "rmq.min.consume.thread.nums";
    String MESSAGE_CONSUME_STATUS = "rmq.message.consume.status";
    String MESSAGE_DESTINATION = "rmq.message.destination";
    String PULL_MESSAGE_BATCH_NUMS = "rmq.pull.message.batch.nums";
    String PULL_MESSAGE_CACHE_CAPACITY = "rmq.pull.message.cache.capacity";
}
