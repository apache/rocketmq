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

package org.apache.rocketmq.store.ha;

import java.net.InetSocketAddress;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * Service to periodically check and notify for certain connection state.
 */
public class HAConnectionStateNotificationService extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long CONNECTION_ESTABLISH_TIMEOUT = 10 * 1000;

    private volatile HAConnectionStateNotificationRequest request;
    private volatile long lastCheckTimeStamp = -1;
    private HAService haService;
    private DefaultMessageStore defaultMessageStore;

    public HAConnectionStateNotificationService(HAService haService, DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + HAConnectionStateNotificationService.class.getSimpleName();
        }
        return HAConnectionStateNotificationService.class.getSimpleName();
    }

    public synchronized void setRequest(HAConnectionStateNotificationRequest request) {
        if (this.request != null) {
            this.request.getRequestFuture().cancel(true);
        }
        this.request = request;
        lastCheckTimeStamp = System.currentTimeMillis();
    }

    private synchronized void doWaitConnectionState() {
        if (this.request == null || this.request.getRequestFuture().isDone()) {
            return;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            if (haService.getHAClient().getCurrentState() == this.request.getExpectState()) {
                this.request.getRequestFuture().complete(true);
                this.request = null;
            } else if (haService.getHAClient().getCurrentState() == HAConnectionState.READY) {
                if ((System.currentTimeMillis() - lastCheckTimeStamp) > CONNECTION_ESTABLISH_TIMEOUT) {
                    LOGGER.error("Wait HA connection establish with {} timeout", this.request.getRemoteAddr());
                    this.request.getRequestFuture().complete(false);
                    this.request = null;
                }
            } else {
                lastCheckTimeStamp = System.currentTimeMillis();
            }
        } else {
            boolean connectionFound = false;
            for (HAConnection connection : haService.getConnectionList()) {
                if (checkConnectionStateAndNotify(connection)) {
                    connectionFound = true;
                }
            }

            if (connectionFound) {
                lastCheckTimeStamp = System.currentTimeMillis();
            }

            if (!connectionFound && (System.currentTimeMillis() - lastCheckTimeStamp) > CONNECTION_ESTABLISH_TIMEOUT) {
                LOGGER.error("Wait HA connection establish with {} timeout", this.request.getRemoteAddr());
                this.request.getRequestFuture().complete(false);
                this.request = null;
            }
        }
    }

    /**
     * Check if connection matched and notify request.
     *
     * @param connection connection to check.
     * @return if connection remote address match request.
     */
    public synchronized boolean checkConnectionStateAndNotify(HAConnection connection) {
        if (this.request == null || connection == null) {
            return false;
        }

        String remoteAddress;
        try {
            remoteAddress = ((InetSocketAddress) connection.getSocketChannel().getRemoteAddress())
                .getAddress().getHostAddress();
            if (remoteAddress.equals(request.getRemoteAddr())) {
                HAConnectionState connState = connection.getCurrentState();

                if (connState == this.request.getExpectState()) {
                    this.request.getRequestFuture().complete(true);
                    this.request = null;
                } else if (this.request.isNotifyWhenShutdown() && connState == HAConnectionState.SHUTDOWN) {
                    this.request.getRequestFuture().complete(false);
                    this.request = null;
                }
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Check connection address exception: {}", e);
        }

        return false;
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(1000);
                this.doWaitConnectionState();
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }
}
