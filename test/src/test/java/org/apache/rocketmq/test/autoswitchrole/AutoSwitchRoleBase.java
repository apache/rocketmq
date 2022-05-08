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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.autoswitchrole;

import java.io.File;
import java.util.UUID;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class AutoSwitchRoleBase {

    private String storePathRootParentDir = System.getProperty("user.home") + File.separator +
        UUID.randomUUID().toString().replace("-", "");
    private String storePathRootDir = storePathRootParentDir + File.separator + "store";

    protected MessageStoreConfig buildMessageStoreConfig(final String brokerName, final int haPort) {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerRole(BrokerRole.SLAVE);
        storeConfig.setHaListenPort(haPort);
        storeConfig.setStorePathRootDir(storePathRootDir + File.separator + brokerName);
        storeConfig.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + File.separator + "commitlog");
        storeConfig.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + File.separator + "EpochFileCache");
        storeConfig.setTotalReplicas(3);
        storeConfig.setInSyncReplicas(2);
        storeConfig.setStartupControllerMode(true);

        storeConfig.setMappedFileSizeCommitLog(1024 * 1024);
        storeConfig.setMappedFileSizeConsumeQueue(1024 * 1024);
        storeConfig.setMaxHashSlotNum(10000);
        storeConfig.setMaxIndexNum(100 * 100);
        storeConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        storeConfig.setFlushIntervalConsumeQueue(1);
        return storeConfig;
    }

    protected ControllerConfig buildControllerConfig(final String id, final String peers) {
        final ControllerConfig config = new ControllerConfig();
        config.setStartupController(true);
        config.setControllerDLegerGroup("group1");
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(id);
        config.setMappedFileSize(1024 * 1024);
        config.setControllerStorePath(storePathRootDir + File.separator + "namesrv" + id + File.separator + "DledgerController");
        return config;
    }

    protected void destroy() {
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

}
