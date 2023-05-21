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
package org.apache.rocketmq.tieredstore.common;

import com.google.common.base.Objects;

public class InFlightRequestKey {

    private final String group;
    private long offset;
    private int batchSize;
    private final long requestTime = System.currentTimeMillis();

    public InFlightRequestKey(String group) {
        this.group = group;
    }

    public InFlightRequestKey(String group, long offset, int batchSize) {
        this.group = group;
        this.offset = offset;
        this.batchSize = batchSize;
    }

    public String getGroup() {
        return group;
    }

    public long getOffset() {
        return offset;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getRequestTime() {
        return requestTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InFlightRequestKey key = (InFlightRequestKey) o;
        return Objects.equal(group, key.group);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(group);
    }
}
