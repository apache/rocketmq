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
package org.apache.rocketmq.common.coldctr;

import java.util.concurrent.atomic.AtomicLong;

public class AccAndTimeStamp {

    public AtomicLong coldAcc = new AtomicLong(0L);
    public Long lastColdReadTimeMills = System.currentTimeMillis();
    public Long createTimeMills = System.currentTimeMillis();

    public AccAndTimeStamp(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
    }

    public AtomicLong getColdAcc() {
        return coldAcc;
    }

    public void setColdAcc(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
    }

    public Long getLastColdReadTimeMills() {
        return lastColdReadTimeMills;
    }

    public void setLastColdReadTimeMills(Long lastColdReadTimeMills) {
        this.lastColdReadTimeMills = lastColdReadTimeMills;
    }

    public Long getCreateTimeMills() {
        return createTimeMills;
    }

    public void setCreateTimeMills(Long createTimeMills) {
        this.createTimeMills = createTimeMills;
    }

    @Override
    public String toString() {
        return "AccAndTimeStamp{" +
            "coldAcc=" + coldAcc +
            ", lastColdReadTimeMills=" + lastColdReadTimeMills +
            ", createTimeMills=" + createTimeMills +
            '}';
    }
}
