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

package org.apache.rocketmq.broker.lite;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

public class LiteTestUtil {

    public static MessageStore buildMessageStore(String storePathRootDir, final BrokerConfig brokerConfig,
        final ConcurrentMap<String, TopicConfig> topicConfigTable, boolean isRocksDBStore) throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        storeConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        storeConfig.setMaxHashSlotNum(10000);
        storeConfig.setMaxIndexNum(100 * 100);
        storeConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        storeConfig.setFlushIntervalConsumeQueue(1);
        storeConfig.setHaListenPort(0);
        storeConfig.setEnableLmq(true);
        storeConfig.setEnableMultiDispatch(true);
        storeConfig.setStorePathRootDir(storePathRootDir);

        BrokerStatsManager brokerStatsManager = new BrokerStatsManager(brokerConfig);
        MessageStore messageStore;
        if (isRocksDBStore) {
            messageStore = new RocksDBMessageStore(storeConfig, brokerStatsManager, null, brokerConfig, topicConfigTable);
        } else {
            messageStore = new DefaultMessageStore(storeConfig, brokerStatsManager, null, brokerConfig, topicConfigTable);
        }
        return messageStore;
    }

    public static MessageExtBrokerInner buildMessage(String parentTopic, String liteTopic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(parentTopic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody("HW".getBytes());
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(new InetSocketAddress("localhost", 10911));
        msg.setBornHost(new InetSocketAddress("localhost", 0));

        if (StringUtils.isNotEmpty(liteTopic)) {
            String lmqName = LiteUtil.toLmqName(parentTopic, liteTopic);
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }
}
