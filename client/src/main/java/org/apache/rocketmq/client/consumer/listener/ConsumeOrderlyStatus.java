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
package org.apache.rocketmq.client.consumer.listener;

/**
 * 消费者顺序消费状态
 */
public enum ConsumeOrderlyStatus {
    /**
     * Success consumption
     * 消费成功
     */
    SUCCESS,
    /**
     * Rollback consumption(only for binlog consumption)
     * 回滚消耗（仅用于binlog消耗）
     */
    @Deprecated
    ROLLBACK,
    /**
     * Commit offset(only for binlog consumption)
     * 提交偏移量（仅用于binlog消耗）
     */
    @Deprecated
    COMMIT,
    /**
     * Suspend current queue a moment
     * 暂时挂起当前队列
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT;
}
