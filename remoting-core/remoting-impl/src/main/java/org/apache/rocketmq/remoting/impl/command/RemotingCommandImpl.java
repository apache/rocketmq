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

package org.apache.rocketmq.remoting.impl.command;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.common.TypePresentation;

public class RemotingCommandImpl implements RemotingCommand {
    public final static RequestIdGenerator REQUEST_ID_GENERATOR = RequestIdGenerator.inst;

    private byte protocolType;
    private byte serializeType;

    private volatile int requestId = REQUEST_ID_GENERATOR.incrementAndGet();
    private TrafficType trafficType = TrafficType.REQUEST_SYNC;
    private String code = CommandFlag.SUCCESS.flag();
    private String remark = "";
    private Map<String, String> properties = new HashMap<String, String>();
    private Object parameter;
    private byte[] extraPayload;

    private byte[] parameterByte;

    protected RemotingCommandImpl() {
    }

    @Override
    public byte protocolType() {
        return this.protocolType;
    }

    @Override
    public void protocolType(byte value) {
        this.protocolType = value;
    }

    @Override
    public int requestID() {
        return requestId;
    }

    @Override
    public void requestID(int value) {
        this.requestId = value;
    }

    @Override
    public byte serializerType() {
        return this.serializeType;
    }

    @Override
    public void serializerType(byte value) {
        this.serializeType = value;
    }

    @Override
    public TrafficType trafficType() {
        return this.trafficType;
    }

    @Override
    public void trafficType(TrafficType value) {
        this.trafficType = value;
    }

    @Override
    public String opCode() {
        return this.code;
    }

    @Override
    public void opCode(String value) {
        this.code = value;
    }

    @Override
    public String remark() {
        return this.remark;
    }

    @Override
    public void remark(String value) {
        this.remark = value;
    }

    @Override
    public Map<String, String> properties() {
        return this.properties;
    }

    @Override
    public void properties(Map<String, String> value) {
        this.properties = value;
    }

    @Override
    public String property(String key) {
        return this.properties.get(key);
    }

    @Override
    public void property(String key, String value) {
        this.properties.put(key, value);
    }

    @Override
    public Object parameter() {
        return this.parameter;
    }

    @Override
    public void parameter(Object value) {
        this.parameter = value;
    }

    @Override
    public byte[] parameterBytes() {
        return this.getParameterByte();
    }

    public byte[] getParameterByte() {
        return parameterByte;
    }

    public void setParameterByte(byte[] parameterByte) {
        this.parameterByte = parameterByte;
    }

    @Override
    public void parameterBytes(byte[] value) {
        this.setParameterByte(value);
    }

    @Override
    public byte[] extraPayload() {
        return this.extraPayload;
    }

    @Override
    public void extraPayload(byte[] value) {
        this.extraPayload = value;
    }

    @Override
    public <T> T parameter(SerializerFactory serializerFactory, Class<T> c) {
        if (this.parameter() != null)
            return (T) this.parameter();
        final T decode = serializerFactory.get(this.serializerType()).decode(this.parameterBytes(), c);
        this.parameter(decode);
        return decode;
    }

    @Override
    public <T> T parameter(SerializerFactory serializerFactory, TypePresentation<T> typePresentation) {
        if (this.parameter() != null)
            return (T) this.parameter();
        final T decode = serializerFactory.get(this.serializerType()).decode(this.parameterBytes(), typePresentation);
        this.parameter(decode);
        return decode;
    }

    @Override
    public <T> T parameter(SerializerFactory serializerFactory, Type type) {
        if (this.parameter() != null)
            return (T) this.parameter();
        final T decode = serializerFactory.get(this.serializerType()).decode(this.parameterBytes(), type);
        this.parameter(decode);
        return decode;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
