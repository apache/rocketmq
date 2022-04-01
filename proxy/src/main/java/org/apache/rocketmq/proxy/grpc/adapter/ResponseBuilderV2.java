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

package org.apache.rocketmq.proxy.grpc.adapter;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import org.apache.rocketmq.common.protocol.ResponseCode;

public class ResponseBuilderV2 {

    public static Status buildStatus(Code code, String message) {
        return Status.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    public static Status buildStatus(int remotingResponseCode, String remark) {
        return Status.newBuilder()
            .setCode(buildCode(remotingResponseCode))
            .setMessage(remark)
            .build();
    }
    
    public static Code buildCode(int remotingResponseCode) {
        Code code;
        switch (remotingResponseCode) {
            case ResponseCode.SUCCESS:
            case ResponseCode.NO_MESSAGE: {
                code = Code.OK;
                break;
            }
            case ResponseCode.SYSTEM_ERROR: {
                code = Code.INTERNAL_SERVER_ERROR;
                break;
            }
            case ResponseCode.SYSTEM_BUSY:
            case ResponseCode.POLLING_FULL: {
                code = Code.TOO_MANY_REQUESTS;
                break;
            }
            case ResponseCode.REQUEST_CODE_NOT_SUPPORTED: {
                code = Code.NOT_IMPLEMENTED;
                break;
            }
            case ResponseCode.MESSAGE_ILLEGAL:
            case ResponseCode.VERSION_NOT_SUPPORTED:
            case ResponseCode.SUBSCRIPTION_PARSE_FAILED:
            case ResponseCode.FILTER_DATA_NOT_EXIST: {
                code = Code.INVALID_ARGUMENT;
                break;
            }
            case ResponseCode.SERVICE_NOT_AVAILABLE:
            case ResponseCode.SLAVE_NOT_AVAILABLE:
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
            case ResponseCode.PULL_OFFSET_MOVED:
            case ResponseCode.SUBSCRIPTION_NOT_LATEST:
            case ResponseCode.FILTER_DATA_NOT_LATEST: {
                code = Code.UNAVAILABLE;
                break;
            }
            case ResponseCode.NO_PERMISSION: {
                code = Code.PERMISSION_DENIED;
                break;
            }
            case ResponseCode.TOPIC_NOT_EXIST:
                code = Code.TOPIC_NOT_FOUND;
                break;
            case ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST:
            case ResponseCode.SUBSCRIPTION_NOT_EXIST:
            case ResponseCode.PULL_NOT_FOUND:
            case ResponseCode.QUERY_NOT_FOUND:
            case ResponseCode.CONSUMER_NOT_ONLINE: {
                code = Code.NOT_FOUND;
                break;
            }
            case ResponseCode.POLLING_TIMEOUT:
            case ResponseCode.FLUSH_DISK_TIMEOUT:
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                code = Code.DEADLINE_EXCEEDED;
                break;
            }
            default: {
                code = Code.INTERNAL_SERVER_ERROR;
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
