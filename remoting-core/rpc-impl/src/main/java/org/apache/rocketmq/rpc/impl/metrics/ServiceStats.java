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

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStats {
    private static final Logger log = LoggerFactory.getLogger("ServiceStats");
    private final StatsItemSet callerOKQPS;
    private final StatsItemSet callerFailedQPS;
    private final StatsItemSet callerRT;
    private final StatsItemSet providerOKQPS;
    private final StatsItemSet providerFailedQPS;
    private final StatsItemSet providerRT;
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("ServiceStatsScheduledThread", true);

    public ServiceStats() {
        this.callerOKQPS = new StatsItemSet("SERVICE_QPS_CALLER_OK", this.scheduledExecutorService, log);
        this.callerFailedQPS = new StatsItemSet("SERVICE_QPS_CALLER_FAILED", this.scheduledExecutorService, log);
        this.callerRT = new StatsItemSet("SERVICE_RT_CALLER", this.scheduledExecutorService, log);
        this.providerOKQPS = new StatsItemSet("SERVICE_QPS_PROVIDER_OK", this.scheduledExecutorService, log);
        this.providerFailedQPS = new StatsItemSet("SERVICE_QPS_PROVIDER_FAILED", this.scheduledExecutorService, log);
        this.providerRT = new StatsItemSet("SERVICE_RT_PROVIDER", this.scheduledExecutorService, log);
    }

    private static MethodStats methodStats(StatsItem itemOK,
        StatsItem itemFailed,
        StatsItem itemRT) {
        MethodStats methodStats = new MethodStats();
        if (itemOK != null) {
            methodStats.setQpsOK(itemOK.getStatsDataInMinute().getTps());
        }
        if (itemFailed != null) {
            methodStats.setFailedTimesInMinutes(itemFailed.getStatsDataInMinute().getSum());
        }
        if (itemRT != null) {
            methodStats.setRtAvgInMinutes(itemRT.getStatsDataInMinute().getAvgpt());
            methodStats.setRtMaxInMinutes(itemRT.getValueMaxInMinutes().get());
            methodStats.setRtMaxIn10Minutes(itemRT.getValueMaxIn10Minutes().get());
            methodStats.setRtMaxInHour(itemRT.getValueMaxInHour().get());
            methodStats.setRtRegion(itemRT.valueRegion());
        }
        return methodStats;
    }

    public void addCallerOKQPSValue(final String statsKey, final int incValue, final int incTimes) {
        this.callerOKQPS.addValue(statsKey, incValue, incTimes);
    }

    public void addCallerFailedQPSValue(final String statsKey, final int incValue, final int incTimes) {
        this.callerFailedQPS.addValue(statsKey, incValue, incTimes);
    }

    public void addCallerRTValue(final String statsKey, final int incValue, final int incTimes) {
        this.callerRT.addValue(statsKey, incValue, incTimes);
    }

    public void addProviderOKQPSValue(final String statsKey, final int incValue, final int incTimes) {
        this.providerOKQPS.addValue(statsKey, incValue, incTimes);
    }

    public void addProviderFailedQPSValue(final String statsKey, final int incValue, final int incTimes) {
        this.providerFailedQPS.addValue(statsKey, incValue, incTimes);
    }

    public void addProviderRTValue(final String statsKey, final int incValue, final int incTimes) {
        this.providerRT.addValue(statsKey, incValue, incTimes);
    }

    public void start() {
        this.callerOKQPS.init();
        this.callerFailedQPS.init();
        this.callerRT.init();
        this.providerOKQPS.init();
        this.providerFailedQPS.init();
        this.providerRT.init();
    }

    public void stop() {
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 3000, TimeUnit.MILLISECONDS);
    }

    StatsAll stats() {
        StatsAll sa = new StatsAll();
        Set<String> keySetCaller = this.callerOKQPS.getStatsItemTable().keySet();
        Set<String> keySetProvider = this.providerOKQPS.getStatsItemTable().keySet();

        for (String statsKey : keySetCaller) {
            MethodStats caller = methodStats(this.callerOKQPS.getStatsItemTable().get(statsKey),
                this.callerFailedQPS.getStatsItemTable().get(statsKey),
                this.callerRT.getStatsItemTable().get(statsKey)
            );
            sa.getStatsCaller().put(statsKey, caller);
        }

        for (String statsKey : keySetProvider) {
            MethodStats provider = methodStats(this.providerOKQPS.getStatsItemTable().get(statsKey),
                this.providerFailedQPS.getStatsItemTable().get(statsKey),
                this.providerRT.getStatsItemTable().get(statsKey)
            );
            sa.getStatsProvider().put(statsKey, provider);
        }
        return sa;
    }
}
