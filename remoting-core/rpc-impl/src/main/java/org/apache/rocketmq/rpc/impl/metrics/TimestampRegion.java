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

package org.apache.rocketmq.rpc.impl.metrics;

public class TimestampRegion {
    private volatile long beginTimestamp = -1;
    private volatile long endTimestamp = -1;

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp = beginTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    @Override
    public int hashCode() {
        int result = (int) (beginTimestamp ^ (beginTimestamp >>> 32));
        result = 31 * result + (int) (endTimestamp ^ (endTimestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimestampRegion that = (TimestampRegion) o;
        return beginTimestamp == that.beginTimestamp && endTimestamp == that.endTimestamp;
    }

    @Override
    public String toString() {
        return "TimestampRegion{" +
            "beginTimestamp=" + beginTimestamp +
            ", endTimestamp=" + endTimestamp +
            '}';
    }
}
