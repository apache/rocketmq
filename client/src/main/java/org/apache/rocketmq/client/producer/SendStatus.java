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
package org.apache.rocketmq.client.producer;

/**
 * 发送消息结果状态
 * 该状态都是消息处理是成功的，只是存储、同步时出现问题。
 */
public enum SendStatus {
    /**
     * 发送成功 and 存储同步成功
     */
    SEND_OK,
    /**
     * 发送成功 but 存储失败
     */
    FLUSH_DISK_TIMEOUT,
    /**
     * 发送成功 but 从节点超时
     */
    FLUSH_SLAVE_TIMEOUT,
    /**
     * 发送成功 but 从节点不可用
     */
    SLAVE_NOT_AVAILABLE,
}
