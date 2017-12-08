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
package io.openmessaging.rocketmq.domain;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.OMS;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BytesMessageImpl implements BytesMessage {
    private KeyValue headers;
    private KeyValue properties;
    private byte[] body;

    public BytesMessageImpl() {
        this.headers = OMS.newKeyValue();
        this.properties = OMS.newKeyValue();
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public BytesMessage setBody(final byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(final String key, final int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(final String key, final long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(final String key, final double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(final String key, final String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(final String key, final int value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(final String key, final long value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(final String key, final double value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(final String key, final String value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
