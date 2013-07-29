package com.alibaba.rocketmq.research.store;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * 测试存储层，简单封装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MessageStoreTestObject {
    // 消息大小
    protected int MessageSize = 1024 * 2;
    // 队列个数
    protected int QUEUE_TOTAL = 1024;
    // 发往哪个队列
    protected AtomicInteger queueId = new AtomicInteger(0);
    // 发送主机地址
    protected SocketAddress bornHost;
    // 存储主机地址
    protected SocketAddress storeHost;
    // 消息体
    protected byte[] messageBody;

    //
    protected final MessageStore messageStore;


    public MessageStoreTestObject(final MessageStoreConfig messageStoreConfig) throws IOException {
        this.storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        this.bornHost = new InetSocketAddress(InetAddress.getByName("10.232.102.184"), 0);
        this.messageStore = new DefaultMessageStore(messageStoreConfig);
        this.messageBody = this.buildMessageBody();
    }


    private byte[] buildMessageBody() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MessageSize; i++) {
            sb.append("Y");
        }

        return sb.toString().getBytes();
    }


    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("AAA");
        msg.setTags("TAG1");
        msg.setKeys(String.valueOf(this.messageStore.now()));
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setSysFlag(4);
        msg.setBornTimestamp(System.currentTimeMillis());

        return msg;
    }


    public boolean load() {
        return this.messageStore.load();
    }


    public void start() throws Exception {
        this.messageStore.start();
    }


    public boolean sendMessage() {
        PutMessageResult result = this.messageStore.putMessage(buildMessage());
        return result != null && result.isOk();
    }


    public void updateMasterAddress(final String addr) {
        this.messageStore.updateHaMasterAddress(addr);
    }


    public long now() {
        return this.messageStore.now();
    }
}
