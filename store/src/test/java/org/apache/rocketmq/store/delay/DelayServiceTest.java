package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DelayServiceTest {

    String clusterName = "delay-cluster";

    private DefaultMessageStore buildStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "delay_test_" + System.currentTimeMillis());
        DefaultMessageStore store = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager(clusterName, true), new MessageArrivingListener() {
            @Override
            public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
            }
        }, new BrokerConfig());
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    store.load();
                    store.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
        t.join();
        return store;
    }

    @Test
    public void testDelayService() throws Exception {
        DefaultMessageStore store = buildStore();

        for (int i = 0; i < 1000; i++) {
            MessageExtBrokerInner msg = createMsg("t1");
            PutMessageResult result = store.putMessage(msg);
            Thread.sleep(500);
        }
        store.shutdown();
    }

    @Test
    public void testAppendMessageAcrossFileBoundary() throws Exception {
        DefaultMessageStore store = buildStore();
        DelayLogService delayLogService = store.delayService.getDelayLogService();

        long fileId = 0L; // 任意固定 fileId，append 与 getMessage 用同一个即可
        int bodySize = 6 * 1024 * 1024; // 6MB
        byte[] body = new byte[bodySize];
        new Random(42).nextBytes(body);

        int msgCount = 12; // 12 * 6MB = 72MB，必跨过 50MB 边界
        long[][] records = new long[msgCount][2]; // [offset, size]

        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner msg = createMsg("t_boundary");
            msg.setBody(body);

            byte[] encoded = MessageDecoder.encode(msg, false);
            long offset = delayLogService.appendMessage(fileId, encoded);
            records[i][0] = offset;
            records[i][1] = encoded.length;
        }

        // 验证每条都能正确取回，body 完整一致
        for (int i = 0; i < msgCount; i++) {
            MessageExt decoded = delayLogService.getMessage(fileId, records[i][0], (int) records[i][1]);
            Assert.assertNotNull("message " + i + " at offset " + records[i][0] + " should be retrievable", decoded);
            Assert.assertArrayEquals("message " + i + " body mismatch (cross-boundary bug?)", body, decoded.getBody());
        }

        store.shutdown();
    }

    private MessageExtBrokerInner createMsg(String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setQueueOffset(0L);
        msg.setSysFlag(0);
        msg.setBody("hello world".getBytes());
        Map<String, String> properties = new HashMap<>();
        MessageAccessor.setProperties(msg, properties);
        msg.setBornTimestamp(System.currentTimeMillis());
        InetSocketAddress address = new InetSocketAddress("10.0.0.11", 12345);
        msg.setBornHost(address);
        msg.setStoreHost(address);
        msg.setReconsumeTimes(0);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CLUSTER, clusterName);
        long fu = System.currentTimeMillis() + 5000;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_DELAY_TIME, fu + "");
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        // msg.setMsgId(MessageClientIDSetter.createUniqID());
        return msg;
    }

}
