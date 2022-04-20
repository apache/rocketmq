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

package org.apache.rocketmq.proxy.grpc.v2.adapter;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.common.protocol.ResponseCode;

public class ResponseBuilder {

    public static Status buildStatus(Throwable t) {
        if (t instanceof CompletionException) {
            t = t.getCause();
        }
        if (t instanceof ProxyException) {
            ProxyException proxyException = (ProxyException) t.getCause();
            return ResponseBuilder.buildStatus(proxyException.getCode(), proxyException.getMessage());
        }
        return ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "internal error");
    }

    public static Status buildStatus(Code code, String message) {
        return Status.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    public static Status buildStatus(int remotingResponseCode, String remark) {
        String message = remark;
        if (message == null) {
            message = String.valueOf(remotingResponseCode);
        }
        return Status.newBuilder()
            .setCode(buildCode(remotingResponseCode))
            .setMessage(message)
            .build();
    }
    
    public static Code buildCode(int remotingResponseCode) {
        switch (remotingResponseCode) {
            case ResponseCode.SUCCESS:
            case ResponseCode.NO_MESSAGE:
            case ResponseCode.PULL_RETRY_IMMEDIATELY: {
                return Code.OK;
            }
            case ResponseCode.SYSTEM_BUSY:
            case ResponseCode.POLLING_FULL: {
                return Code.TOO_MANY_REQUESTS;
            }
            case ResponseCode.REQUEST_CODE_NOT_SUPPORTED: {
                return Code.UNRECOGNIZED;
            }
            case ResponseCode.MESSAGE_ILLEGAL: {
                return Code.ILLEGAL_MESSAGE;
            }
            case ResponseCode.VERSION_NOT_SUPPORTED: {
                return Code.VERSION_UNSUPPORTED;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                return Code.HA_NOT_AVAILABLE;
            }
            case ResponseCode.PULL_OFFSET_MOVED: {
                return Code.ILLEGAL_MESSAGE_OFFSET;
            }
            case ResponseCode.NO_PERMISSION: {
                return Code.FORBIDDEN;
            }
            case ResponseCode.TOPIC_NOT_EXIST: {
                return Code.TOPIC_NOT_FOUND;
            }
            case ResponseCode.PULL_NOT_FOUND: {
                return Code.MESSAGE_NOT_FOUND;
            }
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                return Code.MASTER_PERSISTENCE_TIMEOUT;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                return Code.SLAVE_PERSISTENCE_TIMEOUT;
            }
            default: {
                return Code.INTERNAL_SERVER_ERROR;
            }
        }
    }

    public static String buildMessage(int responseCode, String remark) {
        if (remark != null) {
            return "ResponseCode: " + responseCode + " " + remark;
        }
        return "ResponseCode: " + responseCode;
    }
}
