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
 * 当写一条Message到commit log 后返回的code
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    PUT_OK, // ok
    END_OF_FILE, //文件结尾
    MESSAGE_SIZE_EXCEEDED,//消息大小超长
    PROPERTIES_SIZE_EXCEEDED, //属性的大小超了
    UNKNOWN_ERROR,//不知道的错误
}
