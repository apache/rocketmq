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
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    /**
     * 添加成功
     */
    PUT_OK,
    /**
     * MapperFile 中剩余空间小于当前消息存储空间
     */
    END_OF_FILE,
    /**
     * 消息长度超过配置的消息总长度
     */
    MESSAGE_SIZE_EXCEEDED,
    /**
     * // todo ？？
     */
    PROPERTIES_SIZE_EXCEEDED,
    /**
     * 未知错误
     */
    UNKNOWN_ERROR,
}
