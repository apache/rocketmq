package org.apache.rocketmq.store.dleger;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
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

    private DefaultMessageStore createMessageStore(String base) throws Exception {
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
        storeConfig.setdLegerPeers(String.format("n0-localhost:%d", nextPort()));
        storeConfig.setdLegerSelfId("n0");
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("DLegerCommitlogTest"), new MessageArrivingListener() {

            @Override
            public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                byte[] filterBitMap, Map<String, String> properties) {

            }
        }, new BrokerConfig());
        defaultMessageStore.load();
        defaultMessageStore.start();
        return defaultMessageStore;
    }

    @Test
    public void testPutAndGetMessage() throws Exception {
        String base =  createBaseDir();
        DefaultMessageStore messageStore = createMessageStore(base);
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


}
