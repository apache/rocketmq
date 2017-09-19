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

package org.apache.rocketmq.rpc.impl.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.msgpack.annotation.Message;

@Message
public class RpcServiceCallBody {
    private String serviceId;
    private String serviceVersion;
    private String token;
    private List<byte[]> parameters;

    public RpcServiceCallBody() {
        parameters = new ArrayList<>();
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(final String serviceId) {
        this.serviceId = serviceId;
    }

    public String getToken() {
        return token;
    }

    public void setToken(final String token) {
        this.token = token;
    }

    public byte[] getParameter(int index) {
        return parameters.get(index);
    }

    public List<byte[]> getParameters() {
        return parameters;
    }

    public void setParameters(final List<byte[]> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(final byte[] parameter) {
        parameters.add(parameter);
    }

    public String getServiceVersion() {
        return serviceVersion;
    }

    public void setServiceVersion(final String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
