/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer.listener;

/**
 * 顺序消费，消费结果
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public enum ConsumeOrderlyStatus {
    // 消息处理成功
    SUCCESS,
    // 回滚消息（只供精卫binlog复制使用）
    ROLLBACK,
    // 提交消息（只供精卫binlog复制使用）
    COMMIT,
    // 将当前队列挂起一小会儿
    SUSPEND_CURRENT_QUEUE_A_MOMENT,
}
