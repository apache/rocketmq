package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DelayServiceTest {

    String clusterName = "delay-cluster";


    @Test
    public void testDelayService() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir("/Users/machunxiao/OpenSource/rocketmq/store/target");
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

        for (int i = 0; i < 1000; i++) {
            MessageExtBrokerInner msg = createMsg("t1");
            PutMessageResult result = store.putMessage(msg);
            Thread.sleep(500);
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
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL + MessageConst.PROPERTY_DELAY_TIME_LEVEL, fu + "");
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        // msg.setMsgId(MessageClientIDSetter.createUniqID());
        return msg;
    }

}
