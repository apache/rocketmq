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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Calendar;
import java.util.Map;

import static org.apache.rocketmq.common.message.MessageDecoder.CHARSET_UTF8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreCleanCommitLogServiceTest {
    private DefaultMessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;

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
        int mappedFileSize = 1024;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + "";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 95;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 100 messages
        int mappedFileCountInQueue = buildAndPutMessages(mappedFileSize);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(mappedFileCountInQueue, actualFileSize - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // can comment out the code below, if just run this case rather than debug it.
        Thread.sleep(1000 * 60 + 100);

        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        int actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFileCountInQueue, actualFileSize);

        // Expire 5 files.
        expireSomeFiles(mappedFileSize, storePath, fileReservedTime);

        // delete expired files due to time up
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService = messageStore.new CleanCommitLogService();
        cleanCommitLogService.run();
        actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFileCountInQueue - 5, actualFileSize);
    }

    /**
     * make sure disk space usage is greater than 10%, less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     *
     * @see DefaultMessageStoreCleanCommitLogServiceTest#testDeleteExpiredFilesImmediatelyBySpaceFull()
     */
    @Test
    public void testDeleteExpiredFilesBySpaceFull() throws Exception {
        int mappedFileSize = 1024;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 100 messages
        int mappedFilesCountInQueue = buildAndPutMessages(mappedFileSize);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(mappedFilesCountInQueue, actualFileSize - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();
        // can comment out the code below, if just run this case rather than debug it.
        Thread.sleep(1000 * 60 + 100);

        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        int actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFilesCountInQueue, actualFileSize);

        // Expire 5 files.
        expireSomeFiles(mappedFileSize, storePath, fileReservedTime);

        // delete expired files due to space full
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService = messageStore.new CleanCommitLogService();
        cleanCommitLogService.run();
        actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFilesCountInQueue - 5, actualFileSize);
    }


    /**
     * run with the vm args:
     * -Drocketmq.broker.diskSpaceWarningLevelRatio=0.1
     * -Drocketmq.broker.diskSpaceCleanForciblyRatio=0.1
     * <p>
     * make sure disk space usage is greater than 10%
     */
    @Test
    public void testDeleteExpiredFilesImmediatelyBySpaceFull() throws Exception {
        int mappedFileSize = 1024;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 100 messages
        int mappedFileCountInQueue = buildAndPutMessages(mappedFileSize);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(mappedFileCountInQueue, actualFileSize - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // in this case, Thread.sleep(1000 * 60 + 100) does not work.
        // if want to debug the code, please suspend at line 1217 in DefaultMessageStore.java in another thread
        // Thread.sleep(1000 * 60 + 100);

        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        int actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFileCountInQueue, actualFileSize);

        // Expire 5 files.
        // In this case, there is no need to expire the files.
        // expireSomeFiles(mappedFileSize, storePath, fileReservedTime);

        // delete expired files immediately due to space full
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService = messageStore.new CleanCommitLogService();
        cleanCommitLogService.run();
        actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);

        // use (mappedFileCountInQueue - 10) because of MappedFileQueue.DELETE_FILES_BATCH_MAX equals to 10
        assertEquals(mappedFileCountInQueue - 10, actualFileSize);
    }

    /**
     * make sure disk space usage is less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     */
    @Test
    public void testDeleteExpiredFilesManually() throws Exception {
        int mappedFileSize = 1024;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 95;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        // build and put 100 messages
        int mappedFilesCountInQueue = buildAndPutMessages(mappedFileSize);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(mappedFilesCountInQueue, actualFileSize - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();
        // can comment out the code below, if just run this case rather than debug it.
        Thread.sleep(1000 * 60 + 100);

        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        int actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFilesCountInQueue, actualFileSize);

        // Expire 5 files. ctl.getMappedFiles().size() == 25
        expireSomeFiles(mappedFileSize, storePath, fileReservedTime);

        // Manually delete expired files
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService = messageStore.new CleanCommitLogService();
        // messageStore.executeDeleteFilesManually(); // does not take effect here
        cleanCommitLogService.setManualDeleteFileSeveralTimes(1);
        cleanCommitLogService.run();
        actualFileSize = getMappedFileQueueSize(mappedFileSize, storePath);
        assertEquals(mappedFilesCountInQueue - 5, actualFileSize);
    }

    private int buildAndPutMessages(int mappedFileSize) throws Exception {
        String topic = "test";
        int singleMsgBodyLen = 157;
        // singleMsgLen = 256
        int singleMsgLen = singleMsgBodyLen + topic.getBytes(CHARSET_UTF8).length + 95;
        int msgCount = 100;
        // totalMsgLen = 25600
        int totalMsgLen = singleMsgLen * msgCount;

        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(singleMsgBodyLen, topic);
            PutMessageResult result = messageStore.putMessage(messageExtBrokerInner);
            assertTrue(result != null && result.isOk());
        }

        // wait for build consumer queue completion
        Thread.sleep(100);

        // mappedFilesCountInQueue = 25
        return totalMsgLen % mappedFileSize == 0 ?
                totalMsgLen / mappedFileSize : totalMsgLen / mappedFileSize + 1;
    }

    private void expireSomeFiles(int mappedFileSize, String storePath, int fileReservedTime) {
        MappedFileQueue tempQueue = new MappedFileQueue(storePath, mappedFileSize, null);
        tempQueue.load();

        for (int i = 0; i < tempQueue.getMappedFiles().size(); i++) {
            MappedFile mappedFile = tempQueue.getMappedFiles().get(i);

            int reservedTime = fileReservedTime * 60 * 60 * 1000;
            if (i < 5) {
                boolean modified = mappedFile.getFile().setLastModified(System.currentTimeMillis() - reservedTime * 2);
                assertTrue(modified);
            }
        }

        tempQueue.shutdown(1000);
        tempQueue.destroy();
    }

    private int getMappedFileQueueSize(int mappedFileSize, String storePath) {
        MappedFileQueue tempQueue = new MappedFileQueue(storePath, mappedFileSize, null);
        tempQueue.load();
        int size = tempQueue.getMappedFiles().size();
        tempQueue.shutdown(1000);
        tempQueue.destroy();
        return size;
    }

    private DefaultMessageStore initMessageStore(int mappedFileSize, int fileReservedTime, String deleteWhen, int diskMaxUsedSpaceRatio) throws Exception {
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

        DefaultMessageStore store = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("test"), new MyMessageArrivingListener(), new BrokerConfig());

        assertTrue(store.load());
        store.start();
        return store;
    }

    private MessageExtBrokerInner buildMessage(int singleMsgByte, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(new byte[singleMsgByte]);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        return msg;
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
