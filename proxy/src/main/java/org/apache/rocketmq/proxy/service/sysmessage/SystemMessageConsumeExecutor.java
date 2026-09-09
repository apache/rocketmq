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
package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.config.ProxyConfig;

/** Creates the executor shared by a Proxy's internal system-message consumers. */
public class SystemMessageConsumeExecutor {
    private SystemMessageConsumeExecutor() {
    }

    public static ThreadPoolExecutor create(ProxyConfig config) {
        int coreSize = config.getSystemMessageConsumerThreadPoolCoreSize();
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            coreSize, coreSize,
            1, TimeUnit.MINUTES, "SystemMessageConsumer",
            // LinkedBlockingQueue's default capacity preserves unbounded consumption queueing.
            Integer.MAX_VALUE,
            new ThreadPoolExecutor.AbortPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}
