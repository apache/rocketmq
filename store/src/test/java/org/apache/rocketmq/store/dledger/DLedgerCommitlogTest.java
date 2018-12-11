package org.apache.rocketmq.store.dledger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

public class DLedgerCommitlogTest extends MessageStoreTestBase {


    @Test
    public void testRecover() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false);
            Thread.sleep(1000);
            doPutMessages(messageStore, topic, 0, 1000, 0);
            Thread.sleep(100);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //normal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //abnormal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }
    }



    @Test
    public void testPutAndGetMessage() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false);
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
        messageStore.shutdown();
    }


    @Test
    public void testCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group,"n0", peers, "n0", false);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner =  buildMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.OS_PAGECACHE_BUSY, putMessageResult.getPutMessageStatus());

        Thread.sleep(1000);

        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));


        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group,"n1", peers, "n0", false);
        Thread.sleep(2000);

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(1, followerStore.getMaxOffsetInQueue(topic, 0));


        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }


}
