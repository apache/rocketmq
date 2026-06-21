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

package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TransactionMetricsFlushService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private BrokerController brokerController;
    public TransactionMetricsFlushService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return "TransactionFlushService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service start");
        long start = System.currentTimeMillis();
        while (!this.isStopped()) {
            try {
                // Bug fix: original code only called waitForRunning inside the if-branch,
                // so on every iteration where the interval hadn't elapsed yet the loop
                // spun without yielding (~170 CPU samples in JFR on idle broker).
                // Now we always wait, then check whether enough time has passed to persist.
                long interval = brokerController.getBrokerConfig().getTransactionMetricFlushInterval();
                this.waitForRunning(interval);
                if (System.currentTimeMillis() - start >= interval) {
                    start = System.currentTimeMillis();
                    brokerController.getTransactionalMessageService().getTransactionMetrics().persist();
                }
            } catch (Throwable e) {
                log.error("Error occurred in " + getServiceName(), e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }
}