package org.apache.rocketmq.store.dleger;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Test;

public class DLegerCommitlogTest extends StoreTestBase {

    private DefaultMessageStore createMessageStore(String base, String selfId, String peers, String leaderId) throws Exception {
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
        storeConfig.setdLegerGroup(UUID.randomUUID().toString());
        storeConfig.setdLegerPeers(peers);
        storeConfig.setdLegerSelfId(selfId);
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("DLegerCommitlogTest"), (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {

        }, new BrokerConfig());
        if (leaderId != null) {
            DLegerServer dLegerServer = ((DLegerCommitLog) defaultMessageStore.getCommitLog()).getdLegerServer();
            dLegerServer.getdLegerConfig().setEnableLeaderElector(false);
            if (selfId.equals(leaderId)) {
                dLegerServer.getMemberState().changeToLeader(-1);
            } else {
                dLegerServer.getMemberState().changeToFollower(-1, leaderId);
            }

        }
        defaultMessageStore.load();
        defaultMessageStore.start();
        return defaultMessageStore;
    }

    @Test
    public void testPutAndGetMessage() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = createMessageStore(base, "n0", peers, null);
        Thread.sleep(1000);
        String topic = UUID.randomUUID().toString();

        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msgInner =  buildMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(0);
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        Thread.sleep(100);
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(10, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult =  messageStore.getMessage("group", topic, 0, 0, 32, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(10, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId(), messageExt.getMsgId());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
    }


    @Test
    public void testCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DefaultMessageStore leaderStore = createMessageStore(createBaseDir(), "n0", peers, "n0");

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner =  buildMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, putMessageResult.getPutMessageStatus());

        Thread.sleep(1000);

        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));


        DefaultMessageStore followerStore = createMessageStore(createBaseDir(), "n1", peers, "n0");
        Thread.sleep(2000);

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(1, followerStore.getMaxOffsetInQueue(topic, 0));


        leaderStore.destroy();
        followerStore.destroy();
    }


}
