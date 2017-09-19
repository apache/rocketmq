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

    public static final String PROVIDER_OFF_LINE = "501";
    public static final String CONSUMER_OFF_LINE = "502";
    public static final String PROVIDER_NOT_EXIST = "503";
    public static final String CONSUMER_NOT_EXIST = "504";
    public static final String NAMESPACE_NOT_EXIST = "505";
    public static final String CLIENT_NOT_REGISTERED = "506";
    public static final String NO_SERVICE_ON_LINE = "507";
    public static final String NO_SERVICE_ON_THIS_SERVER = "508";
    public static final String SERVICE_ALREADY_ONLINE = "509";
    public static final String SERVER_NOT_EXIST = "510";
    public static final String SERVER_ALREADY_ONLINE = "511";
    public static final String NO_SERVER_ON_LINE = "512";
    public static final String ACCESS_DENIED = "513";

    public static final String SYSTEM_ERROR = "600";
    public static final String SYSTEM_BUSY = "601";
    public static final String BAD_REQUEST = "602";
    public static final String PARAMETER_ERROR = "603";
    public static final String SEND_REQUEST_FAILED = "604";
    public static final String REQUEST_TIMEOUT = "605";
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
        R_501(501, "Service provider offline "),
        R_502(502, "Service consumer offline"),
        R_503(503, "Service provider not exist"),
        R_504(504, "Service consumer not exist"),
        R_505(505, "Namespace not exist"),
        R_506(506, "Client not registered"),
        R_507(507, "No service online"),
        R_508(508, "No service on this server"),
        R_509(509, "Server already online"),
        R_510(510, "Server not exist"),
        R_511(511, "Server already online"),
        R_512(512, "No server online"),
        R_513(513, "Access denied"),
        R_600(600, "System error"),
        R_601(601, "System busy"),
        R_602(602, "Bad request"),
        R_603(603, "Parameter error"),
        R_604(604, "Send request failed"),
        R_605(605, "Request timeout"),
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
