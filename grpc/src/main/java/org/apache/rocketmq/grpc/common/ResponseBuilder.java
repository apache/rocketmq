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

package org.apache.rocketmq.grpc.common;

import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.rpc.Code;
import com.google.rpc.Status;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ResponseBuilder {
    public static ResponseCommon buildCommon(int responseCode, String remark) {
        Status status = Status.newBuilder()
            .setCode(buildCode(responseCode).getNumber())
            .setMessage(buildMessage(responseCode, remark))
            .build();

        return ResponseCommon.newBuilder()
            .setStatus(status)
            .build();
    }

    public static ResponseCommon buildCommon(Code code, String message) {
        Status status = Status.newBuilder()
            .setCode(code.getNumber())
            .setMessage(message)
            .build();

        return ResponseCommon.newBuilder()
            .setStatus(status)
            .build();
    }

    public static SendMessageResponse buildSendMessageResponse(RemotingCommand command) {
        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) command.readCustomHeader();
        return SendMessageResponse.newBuilder()
            .setCommon(buildCommon(command.getCode(), command.getRemark()))
            .setMessageId(responseHeader.getMsgId())
            .setTransactionId(responseHeader.getTransactionId())
            .build();
    }

    public static Code buildCode(int responseCode) {
        Code code = Code.UNKNOWN;
        switch (responseCode) {
            case ResponseCode.SUCCESS:
            case ResponseCode.NO_MESSAGE: {
                code = Code.OK;
                break;
            }
            case ResponseCode.SYSTEM_ERROR:
            case ResponseCode.FLUSH_DISK_TIMEOUT:
            case ResponseCode.SLAVE_NOT_AVAILABLE:
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                code = Code.INTERNAL;
                break;
            }
            case ResponseCode.SYSTEM_BUSY: {
                code = Code.RESOURCE_EXHAUSTED;
                break;
            }
            case ResponseCode.REQUEST_CODE_NOT_SUPPORTED:
            case ResponseCode.MESSAGE_ILLEGAL:
            case ResponseCode.VERSION_NOT_SUPPORTED: {
                code = Code.INVALID_ARGUMENT;
                break;
            }
            case ResponseCode.SERVICE_NOT_AVAILABLE: {
                code = Code.UNAVAILABLE;
                break;
            }
            case ResponseCode.NO_PERMISSION: {
                code = Code.PERMISSION_DENIED;
                break;
            }
            case ResponseCode.TOPIC_NOT_EXIST: {
                code = Code.NOT_FOUND;
                break;
            }
        }
        return code;
    }

    public static String buildMessage(int responseCode, String remark) {
        if (remark != null) {
            return "ResponseCode: " + responseCode + " " + remark;
        }
        return "ResponseCode: " + responseCode;
    }
}
