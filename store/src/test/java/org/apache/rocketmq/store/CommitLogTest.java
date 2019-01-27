package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * CommitLogTest
 *
 * @author yanglibo@qccr.com
 * @version CommitLogTest.java 2019年01月14日 17:34:31
 */
public class CommitLogTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;

    private MessageStore messageStore;
    private MessageStore slaveMessageStore;
    private MessageStoreConfig masterMessageStoreConfig;
    private MessageStoreConfig slaveStoreConfig;
    private BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest");
    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        masterMessageStoreConfig = new MessageStoreConfig();
        masterMessageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        buildMessageStoreConfig(masterMessageStoreConfig);
        slaveStoreConfig = new MessageStoreConfig();
        slaveStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        slaveStoreConfig.setStorePathRootDir(slaveStoreConfig.getStorePathRootDir()+File.separator+"slave");
        slaveStoreConfig.setStorePathCommitLog(slaveStoreConfig.getStorePathCommitLog()+File.separator+"slave");
        slaveStoreConfig.setHaListenPort(10943);
        buildMessageStoreConfig(slaveStoreConfig);
        messageStore = buildMessageStore(masterMessageStoreConfig,0L);
        slaveMessageStore = buildMessageStore(slaveStoreConfig,1L);
        boolean load = messageStore.load();
        boolean slaveLoad = slaveMessageStore.load();
        slaveMessageStore.updateHaMasterAddress("localhost:10912");
        assertTrue(load);
        assertTrue(slaveLoad);
        messageStore.start();
        slaveMessageStore.start();
    }

    @Test
    public void testHandleHA(){
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = slaveMessageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            assertTrue(GetMessageStatus.FOUND.equals(result.getStatus()));
            result.release();
        }
    }

    @After
    public void destroy() throws Exception{
        slaveMessageStore.shutdown();
        slaveMessageStore.destroy();
        Thread.sleep(10000L);
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(masterMessageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
        file = new File(slaveStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore(MessageStoreConfig messageStoreConfig,long brokerId) throws Exception {

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        return new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig);
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig){
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("myHello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

}
