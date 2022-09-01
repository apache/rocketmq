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

import org.apache.rocketmq.logging.InnerLoggerFactory;

public abstract class AbstractBrokerRunnable implements Runnable {
    protected final BrokerIdentity brokerIdentity;

    public AbstractBrokerRunnable(BrokerIdentity brokerIdentity) {
        this.brokerIdentity = brokerIdentity;
    }

    /**
     * real logic for running
     */
    public abstract void run2();

    @Override
    public void run() {
        if (brokerIdentity.isInBrokerContainer()) {
            // set threadlocal broker identity to forward logging to corresponding broker
            InnerLoggerFactory.BROKER_IDENTITY.set(brokerIdentity.getCanonicalName());
        }
        run2();
    }
}
