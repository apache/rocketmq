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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatService extends ServiceThread {

    private final InternalLogger log = ClientLogger.getLog();
    private final AtomicReference<Runnable> heartbeatTask = new AtomicReference<>();
    private final int heartbeatBrokerInterval;

    public HeartbeatService(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    @Override
    public String getServiceName() {
        return HeartbeatService.class.getSimpleName();
    }

    public void addHeartbeatTask(Runnable task) {
        Runnable expectedTask = heartbeatTask.get();
        while (!heartbeatTask.compareAndSet(expectedTask, task)) {
            expectedTask = heartbeatTask.get();
        }
        wakeup();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                Runnable task = heartbeatTask.getAndSet(null);
                if (task != null) {
                    task.run();
                }
                waitForRunning(heartbeatBrokerInterval);
            } catch (Exception e) {
                log.error("Heartbeat Service Run Method exception", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }
}
