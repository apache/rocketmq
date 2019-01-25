package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 2019/1/23
 */
public class ScheduleMessageServiceTest {



    /** defaultMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h" */
    String testMessageDelayLevel = "5s 10s";

    private static final String storePath = "." + File.separator + "schedule_test";
    private static final int commitLogFileSize = 1024;
    private static final int cqFileSize = 10 * 20;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress BornHost;
    private static SocketAddress StoreHost;
    DefaultMessageStore messageStore;
    MessageStoreConfig messageStoreConfig;
    BrokerConfig brokerConfig;
    ScheduleMessageService scheduleMessageService;



    static {
        try {
            StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    @Before
    public void init() throws Exception {
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel(testMessageDelayLevel);
        messageStoreConfig.setMapedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMapedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        brokerConfig = new BrokerConfig();
        BrokerStatsManager manager = new BrokerStatsManager(brokerConfig.getBrokerClusterName());
        messageStore = new DefaultMessageStore(messageStoreConfig, manager, new MyMessageArrivingListener(), new BrokerConfig());

        assertThat(messageStore.load()).isTrue();


        messageStore.start();
        scheduleMessageService =  messageStore.getScheduleMessageService();
    }



    @Test
    public void computeDeliverTimestampTest(){
         // testMessageDelayLevel  just "5s 10s"
        long storeTime = System.currentTimeMillis();
        long time1 = scheduleMessageService.computeDeliverTimestamp(1,storeTime);
        assertThat(time1).isEqualTo(storeTime+5*1000);

        long time2 = scheduleMessageService.computeDeliverTimestamp(2,storeTime);
        assertThat(time2).isEqualTo(storeTime+10*1000);
    }



    @Test
    public void putDelayMessage() {
        MessageExtBrokerInner msg = buildMessage();
        // set delayTime
        msg.setDelayTimeLevel(1);
        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
    }


    @Test
    public void timerTaskTest(){
        Timer timer = new Timer("ScheduleMessageTimerThreadTest", true);

    }

    @Test
    public void presit(){
        scheduleMessageService.persist();

    }



    @After
    public void shutdown(){
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }



    public MessageExtBrokerInner buildMessage() {
        String message = "schedule message test";
        byte[] msgBody = message.getBytes();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("schedule_topic_test");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }


    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }


    public void getDeliverDelayedMessageTimerTask(int delayLevel,long offset) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        Constructor c = ScheduleMessageService.DeliverDelayedMessageTimerTask.class.getDeclaredConstructor(ScheduleMessageService.class,int.class,long.class);
        c.setAccessible(true);
        ScheduleMessageService.DeliverDelayedMessageTimerTask task = (ScheduleMessageService.DeliverDelayedMessageTimerTask) c.newInstance(scheduleMessageService,1,2);
    }

}
