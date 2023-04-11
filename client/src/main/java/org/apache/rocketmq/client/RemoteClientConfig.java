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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Remoting Client Common configuration
 */
public class RemoteClientConfig {
    private final static Logger log = LoggerFactory.getLogger(MQClientInstance.class);
    /**
     * To decide whether the switch is on or off, from the remoting namesrv.
     */
    private volatile boolean remoteFaultTolerance = true;

    /**
     * Related methods.
     */
    public void setRemoteFaultTolerance(final boolean remoteFaultTolerance) {
        if (this.remoteFaultTolerance != remoteFaultTolerance) {
            log.info("The FaultTolerance switch was modified:" + this.remoteFaultTolerance + "-->" + remoteFaultTolerance);
            this.remoteFaultTolerance = remoteFaultTolerance;
        }
    }

    public boolean getRemoteFaultTolerance() {
        return this.remoteFaultTolerance;
    }

}
