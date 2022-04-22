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
package org.apache.rocketmq.common.protocol.header.namesrv.controller;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 20:01
 */
public enum ErrorCodes {

    NONE((short) 0, "No error"),
    FENCED_LEADER_EPOCH((short) 1, "The leader epoch in the request is older than the current epoch"),
    FENCED_SYNC_STATE_SET_EPOCH((short) 2, "The syncStateSet epoch in the request is older than the current epoch"),
    INVALID_REQUEST((short) 3, "The request is invalid"),
    MASTER_NOT_AVAILABLE((short) 4, "There is no available master for this broker.");

    short code;
    String description;

    ErrorCodes(short code, String description) {
        this.code = code;
        this.description = description;
    }

    public short getCode() {
        return code;
    }
}
