package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Calendar;
import java.util.Map;

import static org.apache.rocketmq.common.message.MessageDecoder.CHARSET_UTF8;
import static org.apache.rocketmq.store.ConsumeQueue.CQ_STORE_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreCleanFilesTest {
    private DefaultMessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private String topic = "test";
    private int queueId = 0;
    private int fileCountCommitLog = 55;
    // exactly one message per CommitLog file.
    private int msgCount = fileCountCommitLog;
    private int mappedFileSize = 128;
    private int fileReservedTime = 1;

    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    /**
     * make sure disk space usage is less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     */
    @Test
    public void testDeleteExpiredFilesByTimeUp() throws Exception {
        String deleteWhen = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + "";
        int diskMaxUsedSpaceRatio = 95; // the max value of diskMaxUsedSpaceRatio
        messageStore = initMessageStore(deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            getCleanCommitLogService().run();
            getCleanConsumeQueueService().run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    /**
     * make sure disk space usage is greater than 10%, less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     *
     * @see DefaultMessageStoreCleanFilesTest#testDeleteFilesImmediatelyBySpaceFull()
     */
    @Test
    public void testDeleteExpiredFilesBySpaceFull() throws Exception {
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;
        messageStore = initMessageStore(deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            getCleanCommitLogService().run();
            getCleanConsumeQueueService().run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }


    /**
     * run with the vm args:
     * -Drocketmq.broker.diskSpaceWarningLevelRatio=0.1
     * -Drocketmq.broker.diskSpaceCleanForciblyRatio=0.1
     * <p>
     * make sure disk space usage is greater than 10%
     */
    @Test
    public void testDeleteFilesImmediatelyBySpaceFull() throws Exception {
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;

        messageStore = initMessageStore(deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        // In this case, there is no need to expire the files.
        // int expireFileCount = 15;
        // expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = fileCountCommitLog;
             a <= (int) Math.ceil((double) fileCountCommitLog / 10) && fileCount >= 10;
             a++, fileCount -= 10) {
            getCleanCommitLogService().run();
            getCleanConsumeQueueService().run();

            assertEquals(fileCountCommitLog - 10 * a, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) (a * 10) / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    /**
     * make sure disk space usage is less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     */
    @Test
    public void testDeleteExpiredFilesManually() throws Exception {
        String deleteWhen = "04";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 95;
        messageStore = initMessageStore(deleteWhen, diskMaxUsedSpaceRatio);
        messageStore.executeDeleteFilesManually();

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            getCleanCommitLogService().run();
            getCleanConsumeQueueService().run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    private DefaultMessageStore.CleanCommitLogService getCleanCommitLogService()
            throws Exception {
        Field serviceField = messageStore.getClass().getDeclaredField("cleanCommitLogService");
        serviceField.setAccessible(true);
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService =
                (DefaultMessageStore.CleanCommitLogService) serviceField.get(messageStore);
        serviceField.setAccessible(false);
        return cleanCommitLogService;
    }

    private DefaultMessageStore.CleanConsumeQueueService getCleanConsumeQueueService()
            throws Exception {
        Field serviceField = messageStore.getClass().getDeclaredField("cleanConsumeQueueService");
        serviceField.setAccessible(true);
        DefaultMessageStore.CleanConsumeQueueService cleanConsumeQueueService =
                (DefaultMessageStore.CleanConsumeQueueService) serviceField.get(messageStore);
        serviceField.setAccessible(false);
        return cleanConsumeQueueService;
    }

    private MappedFileQueue getMappedFileQueueConsumeQueue()
            throws Exception {
        ConsumeQueue consumeQueue = messageStore.getConsumeQueueTable().get(topic).get(queueId);
        Field queueField = consumeQueue.getClass().getDeclaredField("mappedFileQueue");
        queueField.setAccessible(true);
        MappedFileQueue fileQueue = (MappedFileQueue) queueField.get(consumeQueue);
        queueField.setAccessible(false);
        return fileQueue;
    }

    private MappedFileQueue getMappedFileQueueCommitLog() throws Exception {
        CommitLog commitLog = messageStore.getCommitLog();
        Field queueField = commitLog.getClass().getDeclaredField("mappedFileQueue");
        queueField.setAccessible(true);
        MappedFileQueue fileQueue = (MappedFileQueue) queueField.get(commitLog);
        queueField.setAccessible(false);
        return fileQueue;
    }

    private int getFileCountConsumeQueue() {
        int countPerFile = getMsgCountPerConsumeQueueMappedFile();
        double fileCount = (double) msgCount / countPerFile;
        return (int) Math.ceil(fileCount);
    }

    private int getMsgCountPerConsumeQueueMappedFile() {
        int size = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();
        return size / CQ_STORE_UNIT_SIZE;// 7 in this case
    }

    private void buildAndPutMessagesToMessageStore(int msgCount) throws Exception {
        int msgLen = topic.getBytes(CHARSET_UTF8).length + 91;
        int commitLogEndFileMinBlankLength = 4 + 4;
        int singleMsgBodyLen = mappedFileSize - msgLen - commitLogEndFileMinBlankLength;

        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner msg = new MessageExtBrokerInner();
            msg.setTopic(topic);
            msg.setBody(new byte[singleMsgBodyLen]);
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            msg.setQueueId(queueId);
            msg.setSysFlag(0);
            msg.setBornTimestamp(System.currentTimeMillis());
            msg.setStoreHost(storeHost);
            msg.setBornHost(bornHost);
            PutMessageResult result = messageStore.putMessage(msg);
            assertTrue(result != null && result.isOk());
        }

        // wait for build consumer queue completion
        Thread.sleep(100);
    }

    private void expireFiles(MappedFileQueue commitLogQueue, int expireCount) {
        for (int i = 0; i < commitLogQueue.getMappedFiles().size(); i++) {
            MappedFile mappedFile = commitLogQueue.getMappedFiles().get(i);
            int reservedTime = fileReservedTime * 60 * 60 * 1000;
            if (i < expireCount) {
                boolean modified = mappedFile.getFile().setLastModified(System.currentTimeMillis() - reservedTime * 2);
                assertTrue(modified);
            }
        }
    }

    private DefaultMessageStore initMessageStore(String deleteWhen, int diskMaxUsedSpaceRatio) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(mappedFileSize);
        messageStoreConfig.setMapedFileSizeConsumeQueue(mappedFileSize);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);

        // Invalidate DefaultMessageStore`s scheduled task of cleaning expired files.
        // work with the code 'Thread.sleep(1000 * 60 + 100)' behind.
        messageStoreConfig.setCleanResourceInterval(Integer.MAX_VALUE);

        messageStoreConfig.setFileReservedTime(fileReservedTime);
        messageStoreConfig.setDeleteWhen(deleteWhen);
        messageStoreConfig.setDiskMaxUsedSpaceRatio(diskMaxUsedSpaceRatio);

        DefaultMessageStore store = new DefaultMessageStore(messageStoreConfig,
                new BrokerStatsManager("test"), new MyMessageArrivingListener(), new BrokerConfig());

        assertTrue(store.load());
        store.start();
        return store;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }
}
