package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
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

import static java.io.File.separator;
import static org.apache.rocketmq.common.message.MessageDecoder.CHARSET_UTF8;
import static org.apache.rocketmq.store.ConsumeQueue.CQ_STORE_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreCleanFilesTest {
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
        int mappedFileSize = 128;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + "";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 95;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        String topic = "test";
        int queueId = 0;
        String rootDir = messageStore.getMessageStoreConfig().getStorePathRootDir();
        String storePathCommitLog = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        String storePathConsumeQueue = StorePathConfigHelper.getStorePathConsumeQueue(rootDir) + separator + topic + separator + queueId;
        int mappedFileSizeConsumeQueue = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();

        // build and put 50 messages, exactly one message per CommitLog file.
        int fileCountCommitLog = 50;
        int messageCount = fileCountCommitLog;
        buildAndPutMessages(mappedFileSize, messageCount, topic, queueId);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        int actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk);

        int actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        int fileCountConsumeQueue = calculateFileCountConsumeQueue(messageCount);
        assertEquals(fileCountConsumeQueue, actualFileCountConsumeQueueInDisk);

        // Expire 7 files.
        // 7 message info per consumeQueue mappedFile.
        // 1 message per CommitLog mappedFile exactly.
        expireSomeFiles(mappedFileSize, storePathCommitLog, fileReservedTime);

        // delete expired CommitLog files due to time up
        messageStore.new CleanCommitLogService().run();
        actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog - 7, actualFileCountCommitLogInDisk);

        // delete expired ConsumeQueue files due to time up
        messageStore.new CleanConsumeQueueService().run();
        actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        assertEquals(fileCountConsumeQueue - 1, actualFileCountConsumeQueueInDisk);
    }

    /**
     * make sure disk space usage is greater than 10%, less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     *
     * @see DefaultMessageStoreCleanFilesTest#testDeleteExpiredFilesImmediatelyBySpaceFull()
     */
    @Test
    public void testDeleteExpiredFilesBySpaceFull() throws Exception {
        int mappedFileSize = 128;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        String topic = "test";
        int queueId = 0;
        String rootDir = messageStore.getMessageStoreConfig().getStorePathRootDir();
        String storePathCommitLog = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        String storePathConsumeQueue = StorePathConfigHelper.getStorePathConsumeQueue(rootDir) + separator + topic + separator + queueId;
        int mappedFileSizeConsumeQueue = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();

        // build and put 50 messages, exactly one message per CommitLog file.
        int fileCountCommitLog = 50;
        int messageCount = fileCountCommitLog;
        buildAndPutMessages(mappedFileSize, messageCount, topic, queueId);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        int actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk);

        int actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        int fileCountConsumeQueue = calculateFileCountConsumeQueue(messageCount);
        assertEquals(fileCountConsumeQueue, actualFileCountConsumeQueueInDisk);

        // Expire 7 files.
        // 7 message info per consumeQueue mappedFile.
        // 1 message per CommitLog mappedFile exactly.
        expireSomeFiles(mappedFileSize, storePathCommitLog, fileReservedTime);

        // delete expired CommitLog files due to space full
        messageStore.new CleanCommitLogService().run();
        actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog - 7, actualFileCountCommitLogInDisk);

        // delete expired ConsumeQueue files due to space full
        messageStore.new CleanConsumeQueueService().run();
        actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        assertEquals(fileCountConsumeQueue - 1, actualFileCountConsumeQueueInDisk);
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
        int mappedFileSize = 128;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio. make sure disk space usage is greater than 10%
        int diskMaxUsedSpaceRatio = 10;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        String topic = "test";
        int queueId = 0;
        String rootDir = messageStore.getMessageStoreConfig().getStorePathRootDir();
        String storePathCommitLog = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        String storePathConsumeQueue = StorePathConfigHelper.getStorePathConsumeQueue(rootDir) + separator + topic + separator + queueId;
        int mappedFileSizeConsumeQueue = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();

        // build and put 50 messages, exactly one message per CommitLog file.
        int fileCountCommitLog = 50;
        int messageCount = fileCountCommitLog;
        buildAndPutMessages(mappedFileSize, messageCount, topic, queueId);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // if want to debug this case, please suspend at line 1217 in DefaultMessageStore.java in thread level
        // Thread.sleep(1000 * 60 + 100); // does not work here.

        int actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk);

        int actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        int fileCountConsumeQueue = calculateFileCountConsumeQueue(messageCount);
        assertEquals(fileCountConsumeQueue, actualFileCountConsumeQueueInDisk);

        // Expire 7 files.
        // 7 message info per consumeQueue mappedFile.
        // 1 message per CommitLog mappedFile exactly.
        // In this case, there is no need to expire the files.
        // expireSomeFiles(mappedFileSize, storePath, fileReservedTime);

        // delete expired  CommitLog files immediately due to space full
        messageStore.new CleanCommitLogService().run();
        actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        // use (fileCountCommitLog - 10) because of MappedFileQueue.DELETE_FILES_BATCH_MAX equals to 10
        assertEquals(fileCountCommitLog - 10, actualFileCountCommitLogInDisk);

        // delete expired ConsumeQueue files  immediately due to space full
        messageStore.new CleanConsumeQueueService().run();
        actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        assertEquals(fileCountConsumeQueue - 1, actualFileCountConsumeQueueInDisk);
    }

    /**
     * make sure disk space usage is less than 85%.
     * if disk space usage is greater than 85%, by default, it will trigger the deletion at once.
     */
    @Test
    public void testDeleteExpiredFilesManually() throws Exception {
        int mappedFileSize = 128;
        // the number of hours to keep a log file before deleting it (in hours), default is 72
        int fileReservedTime = 1;
        // when to delete,default is at 4 am
        String deleteWhen = "04";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 95;

        // initialize messageStore
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);

        String topic = "test";
        int queueId = 0;
        String rootDir = messageStore.getMessageStoreConfig().getStorePathRootDir();
        String storePathCommitLog = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        String storePathConsumeQueue = StorePathConfigHelper.getStorePathConsumeQueue(rootDir) + separator + topic + separator + queueId;
        int mappedFileSizeConsumeQueue = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();

        // build and put 50 messages, exactly one message per CommitLog file.
        int fileCountCommitLog = 50;
        int messageCount = fileCountCommitLog;
        buildAndPutMessages(mappedFileSize, messageCount, topic, queueId);

        // shutdown and reInit messageStore to clean commitLog files in disk after put messages.
        // use assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk - 1)?
        MessageStore temp = messageStore;
        temp.shutdown();
        messageStore = initMessageStore(mappedFileSize, fileReservedTime, deleteWhen, diskMaxUsedSpaceRatio);
        temp.destroy();

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        int actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog, actualFileCountCommitLogInDisk);

        int actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        int fileCountConsumeQueue = calculateFileCountConsumeQueue(messageCount);
        assertEquals(fileCountConsumeQueue, actualFileCountConsumeQueueInDisk);

        // Expire 7 files.
        // 7 message info per consumeQueue mappedFile.
        // 1 message per CommitLog mappedFile exactly.
        expireSomeFiles(mappedFileSize, storePathCommitLog, fileReservedTime);

        // Manually delete expired CommitLog files
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService = messageStore.new CleanCommitLogService();
        // messageStore.executeDeleteFilesManually(); // does not take effect here
        cleanCommitLogService.setManualDeleteFileSeveralTimes(1);
        cleanCommitLogService.run();
        actualFileCountCommitLogInDisk = getMappedFileCountIndisk(mappedFileSize, storePathCommitLog);
        assertEquals(fileCountCommitLog - 7, actualFileCountCommitLogInDisk);

        // Manually delete expired ConsumeQueue files
        messageStore.new CleanConsumeQueueService().run();
        actualFileCountConsumeQueueInDisk = getMappedFileCountIndisk(mappedFileSizeConsumeQueue, storePathConsumeQueue);
        assertEquals(fileCountConsumeQueue - 1, actualFileCountConsumeQueueInDisk);
    }

    private int calculateFileCountConsumeQueue(int messageCount) {
        int size = messageStore.getMessageStoreConfig().getMapedFileSizeConsumeQueue();
        int eachFileContain = size / CQ_STORE_UNIT_SIZE;
        double fileCount = (double) messageCount / eachFileContain;
        return (int) Math.ceil(fileCount);
    }

    private void buildAndPutMessages(int mappedFileSize, int msgCount, String topic, int queueId) throws Exception {
        // exactly one message per CommitLog file.
        int msgLen = topic.getBytes(CHARSET_UTF8).length + 4 /*TOTALSIZE*/ + 4 /*MAGICCODE*/ + 4 /*BODYCRC*/
                + 4 /*QUEUEID*/ + 4 /*FLAG*/ + 8 /*QUEUEOFFSET*/ + 8 /*PHYSICALOFFSET*/ + 4 /*SYSFLAG*/
                + 8 /*BORNTIMESTAMP*/ + 8 /*BORNHOST*/ + 8 /*STORETIMESTAMP*/ + 8 /*STOREHOSTADDRESS*/
                + 4 /*RECONSUMETIMES*/ + 8 /*Prepared Transaction Offset*/ + 4 /*messagebodyLength*/
                + 1 /*topicLength*/ + 2 /*propertiesLength*/;
        int commitLogEndFileMinBlankLength = 4 + 4;
        int singleMsgBodyLen = mappedFileSize - msgLen - commitLogEndFileMinBlankLength;

        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage(singleMsgBodyLen, topic, queueId);
            PutMessageResult result = messageStore.putMessage(messageExtBrokerInner);
            assertTrue(result != null && result.isOk());
        }

        // wait for build consumer queue completion
        Thread.sleep(100);
    }

    private void expireSomeFiles(int mappedFileSize, String storePath, int fileReservedTime) {
        MappedFileQueue tempQueue = new MappedFileQueue(storePath, mappedFileSize, null);
        tempQueue.load();

        for (int i = 0; i < tempQueue.getMappedFiles().size(); i++) {
            MappedFile mappedFile = tempQueue.getMappedFiles().get(i);
            int reservedTime = fileReservedTime * 60 * 60 * 1000;
            if (i < 7) {
                boolean modified = mappedFile.getFile().setLastModified(System.currentTimeMillis() - reservedTime * 2);
                assertTrue(modified);
            }
        }

        tempQueue.shutdown(1000);
        tempQueue.destroy();
    }

    private int getMappedFileCountIndisk(int mappedFileSize, String storePath) {
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

    private MessageExtBrokerInner buildMessage(int singleMsgByte, String topic, int queueId) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(new byte[singleMsgByte]);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId);
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
