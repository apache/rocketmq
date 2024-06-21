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
package org.apache.rocketmq.tools.admin.common;

public class AdminToolResult<T> {

    private boolean success;
    private int code;
    private String errorMsg;
    private T data;

    public AdminToolResult(boolean success, int code, String errorMsg, T data) {
        this.success = success;
        this.code = code;
        this.errorMsg = errorMsg;
        this.data = data;
    }

    public static AdminToolResult success(Object data) {
        return new AdminToolResult(true, AdminToolsResultCodeEnum.SUCCESS.getCode(), "success", data);
    }

    public static AdminToolResult failure(AdminToolsResultCodeEnum errorCodeEnum, String errorMsg) {
        return new AdminToolResult(false, errorCodeEnum.getCode(), errorMsg, null);
    }

    public static AdminToolResult failure(AdminToolsResultCodeEnum errorCodeEnum, String errorMsg, Object data) {
        return new AdminToolResult(false, errorCodeEnum.getCode(), errorMsg, data);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
