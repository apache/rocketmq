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
package org.apache.rocketmq.acl.plug.entity;

import java.util.concurrent.atomic.AtomicBoolean;

public class LoginInfo {

    private String recognition;

    private long loginTime = System.currentTimeMillis();

    private volatile long operationTime = loginTime;

    private volatile AtomicBoolean clear = new AtomicBoolean();

    private AuthenticationInfo authenticationInfo;

    public AuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(AuthenticationInfo authenticationInfo) {
        this.authenticationInfo = authenticationInfo;
    }

    public String getRecognition() {
        return recognition;
    }

    public void setRecognition(String recognition) {
        this.recognition = recognition;
    }

    public long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(long loginTime) {
        this.loginTime = loginTime;
    }

    public long getOperationTime() {
        return operationTime;
    }

    public void setOperationTime(long operationTime) {
        this.operationTime = operationTime;
    }

    public AtomicBoolean getClear() {
        return clear;
    }

    public void setClear(AtomicBoolean clear) {
        this.clear = clear;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("LoginInfo [recognition=").append(recognition).append(", loginTime=").append(loginTime)
                .append(", operationTime=").append(operationTime).append(", clear=").append(clear)
                .append(", authenticationInfo=").append(authenticationInfo).append("]");
        return builder.toString();
    }

}
