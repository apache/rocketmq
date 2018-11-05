package org.apache.rocketmq.store;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;
import org.junit.After;

public class StoreTestBase {

    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost = new InetSocketAddress("127.0.0.1", 8123);
    private SocketAddress StoreHost = BornHost;
    private byte[] MessageBody = new byte[1024];

    protected Set<String> baseDirs = new HashSet<>();

    private AtomicInteger port = new AtomicInteger(30000);

    public int nextPort() {
        return port.incrementAndGet();
    }

    protected MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("StoreTest");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    public static String createBaseDir() {
        String baseDir = System.getProperty("user.home") + File.separator + "unitteststore" + File.separator + UUID.randomUUID();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    public static void deleteFile(String fileName) {
        deleteFile(new File(fileName));
    }

    public static void deleteFile(File file) {
        UtilAll.deleteFile(file);
    }

    @After
    public void clear() {
        for (String baseDir : baseDirs) {
            deleteFile(baseDir);
        }
    }

}
