/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.broker.latency;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.remoting.netty.RequestTask;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shijia.wxr
 */
public class BrokerFastFailure {
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerFastFailureScheduledThread"));
    private final BrokerController brokerController;

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                doExpiredRequest();
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void doExpiredRequest() {
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                final RequestTask rt = (RequestTask) runnable;
                rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, "broker busy, return failure to client at once");
            } catch (Exception e) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
