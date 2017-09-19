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

package org.apache.rocketmq.rpc.impl.exception;

import org.apache.rocketmq.remoting.api.exception.RemoteAccessException;
import org.apache.rocketmq.rpc.impl.command.ResponseCode;

public class ServiceRuntimeException extends RemoteAccessException {
    private static final String URL = "http://jukola.alibaba.com";
    private String code;
    private String helpUrl;
    private String exMsg;

    public ServiceRuntimeException(String latitude, String code) {
        this(latitude, code, "");
    }

    public ServiceRuntimeException(String latitude, String code, Throwable e) {
        this(latitude, code, e.getMessage());
        if (e.getCause() != null)
            this.setStackTrace(e.getCause().getStackTrace());
        else
            this.setStackTrace(e.getStackTrace());
    }

    public ServiceRuntimeException(String latitude, String code, String msg) {
        super(buildExceptionMsg(latitude, code, msg));
        this.code = code;
        this.exMsg = msg;
        this.helpUrl = buildHelpUrl(latitude, code);
    }

    private static String buildExceptionMsg(String latitude, String code, String msg) {
        StringBuilder sb = new StringBuilder();
        ResponseCode.ResponseStatus responseStatus = ServiceExceptionHandlerCode.searchResponseStatus(Integer.valueOf(code));
        String helpUrl = buildHelpUrl(latitude, code);
        if (responseStatus != null)
            if (msg != null && !msg.isEmpty())
                sb.append(msg).append(", see for more ").append(helpUrl);
            else
                sb.append(responseStatus.getResponseSimpleMessage()).append(", see for more ").append(helpUrl);
        return sb.toString();
    }

    private static String buildHelpUrl(String latitude, String code) {
        return URL + "/" + latitude + "/" + code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getHelpUrl() {
        return helpUrl;
    }

    public void setHelpUrl(String helpUrl) {
        this.helpUrl = helpUrl;
    }

    public String getExMsg() {
        return exMsg;
    }

    public void setExMsg(String exMsg) {
        this.exMsg = exMsg;
    }
}
