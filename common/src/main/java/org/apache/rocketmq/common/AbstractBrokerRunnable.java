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

package org.apache.rocketmq.common;

import java.io.File;
import org.apache.rocketmq.logging.org.slf4j.MDC;

public abstract class AbstractBrokerRunnable implements Runnable {
    protected final BrokerIdentity brokerIdentity;

    public AbstractBrokerRunnable(BrokerIdentity brokerIdentity) {
        this.brokerIdentity = brokerIdentity;
    }

    private static final String MDC_BROKER_CONTAINER_LOG_DIR = "brokerContainerLogDir";

    /**
     * real logic for running
     */
    public abstract void run0();

    @Override
    public void run() {
        try {
            if (brokerIdentity.isInBrokerContainer()) {
                MDC.put(MDC_BROKER_CONTAINER_LOG_DIR, File.separator + brokerIdentity.getCanonicalName());
            }
            run0();
        } finally {
            MDC.clear();
        }
    }
}
