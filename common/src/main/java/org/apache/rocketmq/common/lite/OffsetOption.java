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

package org.apache.rocketmq.common.lite;

import java.util.Objects;

public class OffsetOption {

    public static final long POLICY_LAST_VALUE = 0L;
    public static final long POLICY_MIN_VALUE = 1L;
    public static final long POLICY_MAX_VALUE = 2L;

    private Type type;
    private long value;

    public OffsetOption() {
    }

    public OffsetOption(Type type, long value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OffsetOption option = (OffsetOption) o;
        return value == option.value && type == option.type;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(type);
        result = 31 * result + Long.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "OffsetOption{" + "type=" + type +
            ", value=" + value +
            '}';
    }

    public enum Type {
        POLICY,
        OFFSET,
        TAIL_N,
        TIMESTAMP
    }

}