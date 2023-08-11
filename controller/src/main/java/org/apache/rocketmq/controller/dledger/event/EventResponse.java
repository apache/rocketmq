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

package org.apache.rocketmq.controller.dledger.event;

import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class EventResponse<T extends EventResult> {
    private T responseResult;

    private int responseCode = ResponseCode.SUCCESS;

    private String responseMsg;

    public EventResponse() {
        responseResult = null;
    }

    public EventResponse(T responseResult) {
        this.responseResult = responseResult;
    }

    public void setResponse(int code, String responseMsg) {
        this.responseCode = code;
        this.responseMsg = responseMsg;
    }

    public void setResponseResult(T responseResult) {
        this.responseResult = responseResult;
    }

    public T getResponseResult() {
        return responseResult;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseMsg() {
        return responseMsg;
    }

    public void setResponseMsg(String responseMsg) {
        this.responseMsg = responseMsg;
    }

    @Override
    public String toString() {
        return "EventResponse{" +
            "responseResult=" + responseResult +
            ", responseCode=" + responseCode +
            ", responseMsg='" + responseMsg + '\'' +
            '}';
    }
}
