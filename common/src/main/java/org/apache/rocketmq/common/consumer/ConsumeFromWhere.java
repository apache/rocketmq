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
package org.apache.rocketmq.common.consumer;

/**
 * 从哪里消费
 */
public enum ConsumeFromWhere {

    // 从最后一个偏移量消耗
    CONSUME_FROM_LAST_OFFSET,

    // 你第一次启动的时候，消耗\从最后一个\偏移\和\从\最小\的\u
    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,

    // 从最小值偏移量消耗
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,

    // 从最大偏移量消耗
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,

    // 从第一个偏移量消耗
    CONSUME_FROM_FIRST_OFFSET,

    // 使用时间戳消耗
    CONSUME_FROM_TIMESTAMP,
}
