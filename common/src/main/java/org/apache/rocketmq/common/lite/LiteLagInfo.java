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

public class LiteLagInfo {
    private String liteTopic;
    private long lagCount;
    // earliest unconsumed timestamp
    private long earliestUnconsumedTimestamp = -1;

    public String getLiteTopic() {
        return liteTopic;
    }

    public void setLiteTopic(String liteTopic) {
        this.liteTopic = liteTopic;
    }

    public long getLagCount() {
        return lagCount;
    }

    public void setLagCount(long lagCount) {
        this.lagCount = lagCount;
    }

    public long getEarliestUnconsumedTimestamp() {
        return earliestUnconsumedTimestamp;
    }

    public void setEarliestUnconsumedTimestamp(long earliestUnconsumedTimestamp) {
        this.earliestUnconsumedTimestamp = earliestUnconsumedTimestamp;
    }
}
