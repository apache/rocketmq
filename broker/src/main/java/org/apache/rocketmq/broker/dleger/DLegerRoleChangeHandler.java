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
package org.apache.rocketmq.broker.dleger;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.dleger.DLegerLeaderElector;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;

public class DLegerRoleChangeHandler implements DLegerLeaderElector.RoleChangeHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private BrokerController brokerController;
    private DefaultMessageStore messageStore;
    public DLegerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
    }

    @Override public void handle(long term, MemberState.Role role) {
        long start = System.currentTimeMillis();
        try {
            log.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
            switch (role) {
                case CANDIDATE:
                    if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                        brokerController.changeToSlave();
                    }
                    break;
                case FOLLOWER:
                    brokerController.changeToSlave();
                    break;
                case LEADER:
                    while (messageStore.dispatchBehindBytes() != 0) {
                        Thread.sleep(100);
                    }
                    messageStore.recoverTopicQueueTable();
                    brokerController.changeToMaster(BrokerRole.SYNC_MASTER);
                    break;
                default:
                    break;
            }
            log.info("Finish handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), UtilAll.elapsed(start));
        } catch (Throwable t) {
            log.info("Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), UtilAll.elapsed(start), t);
        }
    }
}
