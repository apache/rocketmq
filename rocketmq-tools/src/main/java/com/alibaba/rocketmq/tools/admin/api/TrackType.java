/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.tools.admin.api;

public enum TrackType {
    // 已订阅，并且消息已被消费
    CONSUMED,
    // 已订阅，但消息被过滤表达式过滤
    CONSUMED_BUT_FILTERED,
    // 以 PULL 方式订阅，消费位点完全由应用方控制
    PULL,
    // 已订阅，但消息未被消费
    NOT_CONSUME_YET,
    // 已订阅，但是订阅组当前不在线
    NOT_ONLINE,
    // 未知异常，请查看 url
    UNKNOWN,
}
