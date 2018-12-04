package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import java.io.File;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;

public class MessageStoreTestBase extends StoreTestBase {

    protected DefaultMessageStore createDledgerMessageStore(String base, String group, String selfId, String peers, String leaderId, boolean createAbort) throws Exception {
        baseDirs.add(base);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMapedFileSizeCommitLog(1024 * 100);
        storeConfig.setMapedFileSizeConsumeQueue(1024);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(base);
        storeConfig.setStorePathCommitLog(base + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);

        storeConfig.setEnableDLegerCommitLog(true);
        storeConfig.setdLegerGroup(group);
        storeConfig.setdLegerPeers(peers);
        storeConfig.setdLegerSelfId(selfId);
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("DLedgerCommitlogTest"), (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {

        }, new BrokerConfig());
        if (leaderId != null) {
            DLedgerServer dLegerServer = ((DLedgerCommitLog) defaultMessageStore.getCommitLog()).getdLedgerServer();
            dLegerServer.getdLedgerConfig().setEnableLeaderElector(false);
            if (selfId.equals(leaderId)) {
                dLegerServer.getMemberState().changeToLeader(-1);
            } else {
                dLegerServer.getMemberState().changeToFollower(-1, leaderId);
            }

        }
        if (createAbort) {
            String fileName = StorePathConfigHelper.getAbortFile(storeConfig.getStorePathRootDir());
            makeSureFileExists(fileName);
        }
        Assert.assertTrue(defaultMessageStore.load());
        defaultMessageStore.start();
        return defaultMessageStore;
    }


    protected DefaultMessageStore createMessageStore(String base, boolean createAbort) throws Exception {
        baseDirs.add(base);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMapedFileSizeCommitLog(1024 * 100);
        storeConfig.setMapedFileSizeConsumeQueue(1024);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(base);
        storeConfig.setStorePathCommitLog(base + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("CommitlogTest"), (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {

        }, new BrokerConfig());

        if (createAbort) {
            String fileName = StorePathConfigHelper.getAbortFile(storeConfig.getStorePathRootDir());
            makeSureFileExists(fileName);
        }
        Assert.assertTrue(defaultMessageStore.load());
        defaultMessageStore.start();
        return defaultMessageStore;
    }

    protected void doPutMessages(MessageStore messageStore, String topic, int queueId, int num, long beginLogicsOffset) {
        for (int i = 0; i < num; i++) {
            MessageExtBrokerInner msgInner = buildMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(queueId);
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(beginLogicsOffset + i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
    }

}
