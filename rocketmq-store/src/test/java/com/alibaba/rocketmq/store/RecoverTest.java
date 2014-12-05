/**
 * $Id: RecoverTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


public class RecoverTest {
    // 队列个数
    private static int QUEUE_TOTAL = 10;
    // 发往哪个队列
    private static AtomicInteger QueueId = new AtomicInteger(0);
    // 发送主机地址
    private static SocketAddress BornHost;
    // 存储主机地址
    private static SocketAddress StoreHost;
    // 消息体
    private static byte[] MessageBody;

    private static final String StoreMessage = "Once, there was a chance for me!aaaaaaaaaaaaaaaaaaaaaaaa";


    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("TOPIC_A");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(4);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);

        return msg;
    }


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    private MessageStore storeWrite1;
    private MessageStore storeWrite2;
    private MessageStore storeRead;


    private void destroy() {
        if (storeWrite1 != null) {
            // 关闭存储服务
            storeWrite1.shutdown();
            // 删除文件
            storeWrite1.destroy();
        }

        if (storeWrite2 != null) {
            // 关闭存储服务
            storeWrite2.shutdown();
            // 删除文件
            storeWrite2.destroy();
        }

        if (storeRead != null) {
            // 关闭存储服务
            storeRead.shutdown();
            // 删除文件
            storeRead.destroy();
        }
    }


    public void writeMessage(boolean normal, boolean first) throws Exception {
        System.out.println("================================================================");
        long totalMsgs = 1000;
        QUEUE_TOTAL = 3;

        // 构造消息体
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        // 每个物理映射文件
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 32);
        // 每个逻辑映射文件
        messageStoreConfig.setMapedFileSizeConsumeQueue(100 * 20);
        messageStoreConfig.setMessageIndexEnable(false);

        MessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null);
        if (first) {
            this.storeWrite1 = messageStore;
        }
        else {
            this.storeWrite2 = messageStore;
        }

        // 第一步，load已有数据
        boolean loadResult = messageStore.load();
        assertTrue(loadResult);

        // 第二步，启动服务
        messageStore.start();

        // 第三步，发消息
        for (long i = 0; i < totalMsgs; i++) {

            PutMessageResult result = messageStore.putMessage(buildMessage());

            System.out.println(i + "\t" + result.getAppendMessageResult().getMsgId());
        }

        if (normal) {
            // 关闭存储服务
            messageStore.shutdown();
        }

        System.out.println("========================writeMessage OK========================================");
    }


    private void veryReadMessage(int queueId, long queueOffset, List<ByteBuffer> byteBuffers) {
        for (ByteBuffer byteBuffer : byteBuffers) {
            MessageExt msg = MessageDecoder.decode(byteBuffer);
            System.out.println("request queueId " + queueId + ", request queueOffset " + queueOffset
                    + " msg queue offset " + msg.getQueueOffset());

            assertTrue(msg.getQueueOffset() == queueOffset);

            queueOffset++;
        }
    }


    public void readMessage(final long msgCnt) throws Exception {
        System.out.println("================================================================");
        QUEUE_TOTAL = 3;

        // 构造消息体
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        // 每个物理映射文件
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 32);
        // 每个逻辑映射文件
        messageStoreConfig.setMapedFileSizeConsumeQueue(100 * 20);
        messageStoreConfig.setMessageIndexEnable(false);

        storeRead = new DefaultMessageStore(messageStoreConfig, null);
        // 第一步，load已有数据
        boolean loadResult = storeRead.load();
        assertTrue(loadResult);

        // 第二步，启动服务
        storeRead.start();

        // 第三步，收消息
        long readCnt = 0;
        for (int queueId = 0; queueId < QUEUE_TOTAL; queueId++) {
            for (long offset = 0;;) {
                GetMessageResult result =
                        storeRead.getMessage("GROUP_A", "TOPIC_A", queueId, offset, 1024 * 1024, null);
                if (result.getStatus() == GetMessageStatus.FOUND) {
                    System.out.println(queueId + "\t" + result.getMessageCount());
                    this.veryReadMessage(queueId, offset, result.getMessageBufferList());
                    offset += result.getMessageCount();
                    readCnt += result.getMessageCount();
                    result.release();
                }
                else {
                    break;
                }
            }
        }

        System.out.println("readCnt = " + readCnt);
        assertTrue(readCnt == msgCnt);

        System.out.println("========================readMessage OK========================================");
    }


    /**
     * 正常关闭后，重启恢复消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_normally() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }


    /**
     * 正常关闭后，重启恢复消息，并再次写入消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_normally_write() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(true, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }


    /**
     * 异常关闭后，重启恢复消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_abnormally() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }


    /**
     * 异常关闭后，重启恢复消息，并再次写入消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_abnormally_write() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(false, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }
}
