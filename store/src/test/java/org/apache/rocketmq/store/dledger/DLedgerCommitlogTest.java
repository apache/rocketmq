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
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
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
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExtBatch;
mport static org.assertj.core.api.Assertions.assertThat;


public class DLedgerCommitlogTest extends MessageStoreTestBase {
 
    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;
    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    @Test
    public void testTruncateCQ() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(2000);
            doPutMessages(messageStore, topic, 0, 2000, 0);
            Thread.sleep(100);
            Assert.assertEquals(24, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 2000, 0);
            messageStore.shutdown();
        }

        {
            //Abnormal recover, left some commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 4);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(1000);
            Assert.assertEquals(20, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1700, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1700, 0);
            messageStore.shutdown();
        }
        {
            //Abnormal recover, left none commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 20);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(1000);
            Assert.assertEquals(0, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            messageStore.shutdown();
        }
    }



    @Test
    public void testRecover() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
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
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //abnormal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
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
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
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
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group,"n0", peers, "n0", false, 0);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner =  buildMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.OS_PAGECACHE_BUSY, putMessageResult.getPutMessageStatus());

        Thread.sleep(1000);

        Assert.assertEquals(0, leaderStore.getCommitLog().getMaxOffset());
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));


        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group,"n1", peers, "n0", false, 0);
        Thread.sleep(2000);

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(1, followerStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);


        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }

        @Test
    public void testPutBatchMessageBatch() throws Exception{
        String base=createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        Thread.sleep(1000);
        String topic = UUID.randomUUID().toString();
        List<Message> messages = new ArrayList<>();
        //String topic = "batch-write-topic";
        int queue = 0;
        int[] msgLengthArr = new int[11];
        msgLengthArr[0] = 0;
        int j = 1;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody(("body" + i).getBytes());
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
            String properties = messageProperties2String(msg.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short) propertiesBytes.length;
            final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            msgLengthArr[j] = calMsgLength(msg.getBody().length, topicLength, propertiesLength) + msgLengthArr[j - 1];
            j++;
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));
        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        assertThat(putMessageResult.isOk()).isTrue();
        Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
    }

    public String messageProperties2String(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        if (properties != null) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String name = entry.getKey();
                final String value = entry.getValue();

                sb.append(name);
                sb.append(NAME_VALUE_SEPARATOR);
                sb.append(value);
                sb.append(PROPERTY_SEPARATOR);
            }
        }
        return sb.toString();
    }

    private int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

}
