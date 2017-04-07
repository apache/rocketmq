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
package org.apache.rocketmq.store;

/**
 * Put Message（添加/投掷消息）结果状态
 */
public enum PutMessageStatus {
    /**
     * 成功
     */
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    /**
     * 创建映射文件失败
     */
    @SuppressWarnings("SpellCheckingInspection")
    CREATE_MAPEDFILE_FAILED,
    /**
     * 消息格式不正确，例如消息过长
     */
    MESSAGE_ILLEGAL,
    /**
     * 消息properties属性过长
     */
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGECACHE_BUSY,
    /**
     * 未知错误
     */
    UNKNOWN_ERROR,
}
