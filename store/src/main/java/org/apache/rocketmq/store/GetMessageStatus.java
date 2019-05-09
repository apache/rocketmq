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

public enum GetMessageStatus {

    FOUND,    //找到消息

    NO_MATCHED_MESSAGE, //没有匹配到

    MESSAGE_WAS_REMOVING, //消息已经被移除

    OFFSET_FOUND_NULL,    //消息存放到下一个commitLog 文件中

    OFFSET_OVERFLOW_BADLY, //offset 越界

    OFFSET_OVERFLOW_ONE,   //offset 越界

    OFFSET_TOO_SMALL,      //offset 未在队列中

    NO_MATCHED_LOGIC_QUEUE, //未找到队列

    NO_MESSAGE_IN_QUEUE,    //未在队列中找到消息
}
