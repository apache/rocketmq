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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public interface HAService {

    /**
     * Init HAService, must be called before other methods.
     *
     * @param defaultMessageStore
     * @throws IOException
     */
    void init(DefaultMessageStore defaultMessageStore) throws IOException;

    /**
     * Start HA Service
     *
     * @throws Exception
     */
    void start() throws Exception;

    /**
     * Shutdown HA Service
     */
    void shutdown();

    /**
     * Change to master state
     *
     * @param masterEpoch the new masterEpoch
     */
    default boolean changeToMaster(int masterEpoch) {
        return false;
    }

    /**
     * Change to slave state
     *
     * @param newMasterAddr new master addr
     * @param newMasterEpoch new masterEpoch
     */
    default boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        return false;
    }

    /**
     * Update master address
     *
     * @param newAddr
     */
    void updateMasterAddress(String newAddr);

    /**
     * Update ha master address
     *
     * @param newAddr
     */
    void updateHaMasterAddress(String newAddr);

    /**
     * Returns the number of replicas those commit log are not far behind the master. It includes master itself. Returns
     * syncStateSet size if HAService instanceof AutoSwitchService
     *
     * @return the number of slaves
     * @see MessageStoreConfig#getHaMaxGapNotInSync()
     */
    int inSyncReplicasNums(long masterPutWhere);

    /**
     * Get connection count
     *
     * @return the number of connection
     */
    AtomicInteger getConnectionCount();

    /**
     * Put request to handle HA
     *
     * @param request
     */
    void putRequest(final CommitLog.GroupCommitRequest request);

    /**
     * Put GroupConnectionStateRequest for preOnline
     *
     * @param request
     */
    void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request);

    /**
     * Get ha connection list
     *
     * @return List<HAConnection>
     */
    List<HAConnection> getConnectionList();

    /**
     * Get HAClient
     *
     * @return HAClient
     */
    HAClient getHAClient();

    /**
     * Get the max offset in all slaves
     */
    AtomicLong getPush2SlaveMaxOffset();

    /**
     * Get HA runtime info
     */
    HARuntimeInfo getRuntimeInfo(final long masterPutWhere);

    /**
     * Get WaitNotifyObject
     */
    WaitNotifyObject getWaitNotifyObject();

    /**
     * Judge whether the slave keeps up according to the masterPutWhere, If the offset gap exceeds haSlaveFallBehindMax,
     * then slave is not OK
     */
    boolean isSlaveOK(long masterPutWhere);
}
