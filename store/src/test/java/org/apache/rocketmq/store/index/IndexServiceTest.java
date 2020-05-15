package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import static org.junit.Assert.*;

public class IndexServiceTest {

    private DefaultMessageStore initDefaultMessageStore() throws Exception {
        DefaultMessageStore store = buildMessageStore();

        return store;
    }

    private DefaultMessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), null, new BrokerConfig());
    }

    @Test
    public void getAndCreateLastIndexFile() throws Exception {
    }

    @Test
    public void retryGetAndCreateIndexFile() throws Exception {

        DefaultMessageStore store = initDefaultMessageStore();
        IndexService indexService = new IndexService(store);
        IndexFile indexFile = indexService.retryGetAndCreateIndexFile();
        assertNotNull(indexFile);
    }

}