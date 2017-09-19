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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class MethodStats {
    private double qpsOK;
    private long failedTimesInMinutes;
    private double rtAvgInMinutes;
    private long rtMaxInMinutes;
    private long rtMaxIn10Minutes;
    private long rtMaxInHour;
    private long[] rtRegion;

    public double getQpsOK() {
        return qpsOK;
    }

    public void setQpsOK(double qpsOK) {
        this.qpsOK = qpsOK;
    }

    public long getFailedTimesInMinutes() {
        return failedTimesInMinutes;
    }

    public void setFailedTimesInMinutes(long failedTimesInMinutes) {
        this.failedTimesInMinutes = failedTimesInMinutes;
    }

    public double getRtAvgInMinutes() {
        return rtAvgInMinutes;
    }

    public void setRtAvgInMinutes(double rtAvgInMinutes) {
        this.rtAvgInMinutes = rtAvgInMinutes;
    }

    public long getRtMaxInMinutes() {
        return rtMaxInMinutes;
    }

    public void setRtMaxInMinutes(long rtMaxInMinutes) {
        this.rtMaxInMinutes = rtMaxInMinutes;
    }

    public long getRtMaxIn10Minutes() {
        return rtMaxIn10Minutes;
    }

    public void setRtMaxIn10Minutes(long rtMaxIn10Minutes) {
        this.rtMaxIn10Minutes = rtMaxIn10Minutes;
    }

    public long getRtMaxInHour() {
        return rtMaxInHour;
    }

    public void setRtMaxInHour(long rtMaxInHour) {
        this.rtMaxInHour = rtMaxInHour;
    }

    public long[] getRtRegion() {
        return rtRegion;
    }

    public void setRtRegion(long[] rtRegion) {
        this.rtRegion = rtRegion;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
