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

package org.apache.rocketmq.rpc.impl.command;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * 1xx: SDK exception
 * <p>
 * 4xx: User exception
 * <p>
 * 5xx: Server exception
 * <p>
 * 6xx: Common exception
 */
public class ResponseCode {

    public static final String ILLEGAL_ACCESS = "100";
    public static final String NULL_POINTER = "102";
    public static final String FAIL_INVOKE = "103";
    public static final String INSTANTIATED_FAIL = "104";
    public static final String SUCCESS = "0";

    public static final String USER_SERVICE_EXCEPTION = "400";
    public static final String USER_EXCEPTION_CLASS_NOT_FOUND = "401";
    public static final String USER_EXCEPTION_METHOD_NOT_FOUND = "402";


    public static final String PARAMETER_ERROR = "603";
    public static final String UNKNOWN_EXCEPTION = "606";

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public enum ResponseStatus {
        R_100(100, "Illegal access"),
        R_101(101, "Illegal argument"),
        R_102(102, "Null pointer"),
        R_103(103, "Invoke failed"),
        R_104(104, "Failed initialization"),
        R_200(200, "Success"),
        R_400(400, "User service happened"),
        R_401(401, "Class not found"),
        R_402(402, "Method not found"),

        R_603(603, "Parameter error"),
        R_606(606, "unknown exception");

        private int responseCode = 0;
        private String responseSimpleMessage = "";

        ResponseStatus(int responseCode, String responseSimpleMessage) {
            this.responseCode = responseCode;
            this.responseSimpleMessage = responseSimpleMessage;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public String getResponseSimpleMessage() {
            return responseSimpleMessage;
        }
    }
}
