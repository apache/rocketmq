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

package org.apache.rocketmq.remoting.api.command;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.common.TypePresentation;

public interface RemotingCommand {
    byte protocolType();

    void protocolType(byte value);

    int requestID();

    void requestID(int value);

    byte serializerType();

    void serializerType(byte value);

    TrafficType trafficType();

    void trafficType(TrafficType value);

    String opCode();

    void opCode(String value);

    String remark();

    void remark(String value);

    Map<String, String> properties();

    void properties(Map<String, String> value);

    String property(String key);

    void property(String key, String value);

    Object parameter();

    void parameter(Object value);

    byte[] parameterBytes();

    void parameterBytes(byte[] value);

    byte[] extraPayload();

    void extraPayload(byte[] value);

    <T> T parameter(final SerializerFactory serializerFactory, Class<T> c);

    <T> T parameter(final SerializerFactory serializerFactory, final TypePresentation<T> typePresentation);

    <T> T parameter(final SerializerFactory serializerFactory, final Type type);

    enum CommandFlag {
        SUCCESS("0"),
        ERROR("-1");

        private String flag;

        CommandFlag(final String flag) {
            this.flag = flag;
        }

        public String flag() {
            return flag;
        }
    }
}