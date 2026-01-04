/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.kv;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.MessageExtEncoder;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.SparseConsumeQueue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.store.kv.CompactionLog.COMPACTING_SUB_FOLDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompactionLogTest {
    CompactionLog clog;
    MessageStoreConfig storeConfig;
    MessageStore defaultMessageStore;
    CompactionPositionMgr positionMgr;
    String topic = "ctopic";
    int queueId = 0;
    int offsetMemorySize = 1024;
    int compactionFileSize = 10240;
    int compactionCqFileSize = 1024;


    private static MessageExtEncoder encoder = new MessageExtEncoder(1024, new MessageStoreConfig());
    private static SocketAddress storeHost;
    private static SocketAddress bornHost;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    String logPath;
    String cqPath;

    static {
        try {
            storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
        }
        try {
            bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
        }
    }

    @Before
    public void setUp() throws IOException {
        File file = tmpFolder.newFolder("compaction");
        logPath = Paths.get(file.getAbsolutePath(), "compactionLog").toString();
        cqPath = Paths.get(file.getAbsolutePath(), "compactionCq").toString();

        storeConfig = mock(MessageStoreConfig.class);
        doReturn(compactionFileSize).when(storeConfig).getCompactionMappedFileSize();
        doReturn(compactionCqFileSize).when(storeConfig).getCompactionCqMappedFileSize();
        defaultMessageStore = mock(DefaultMessageStore.class);
        doReturn(storeConfig).when(defaultMessageStore).getMessageStoreConfig();
        positionMgr = mock(CompactionPositionMgr.class);
        doReturn(-1L).when(positionMgr).getOffset(topic, queueId);
    }

    static int queueOffset = 0;
    static int keyCount = 10;
    public static ByteBuffer buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("ctopic");
        msg.setTags(System.currentTimeMillis() + "TAG");
        msg.setKeys(String.valueOf(queueOffset % keyCount));
        msg.setBody(RandomStringUtils.randomAlphabetic(100).getBytes(StandardCharsets.UTF_8));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setQueueOffset(queueOffset);
        queueOffset++;
        for (int i = 1; i < 3; i++) {
            msg.putUserProperty(String.valueOf(i), "xxx" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        encoder.encode(msg);
        return encoder.getEncoderBuffer();
    }


    @Test
    public void testCheck() throws IllegalAccessException {
        MappedFileQueue mfq = mock(MappedFileQueue.class);
        MappedFileQueue smfq = mock(MappedFileQueue.class);
        SparseConsumeQueue scq = mock(SparseConsumeQueue.class);
        doReturn(smfq).when(scq).getMappedFileQueue();
        CompactionLog.TopicPartitionLog tpLog = mock(CompactionLog.TopicPartitionLog.class);
        FieldUtils.writeField(tpLog, "mappedFileQueue", mfq, true);
        FieldUtils.writeField(tpLog, "consumeQueue", scq, true);

        doReturn(Lists.newArrayList()).when(mfq).getMappedFiles();
        doReturn(Lists.newArrayList()).when(smfq).getMappedFiles();

        doCallRealMethod().when(tpLog).sanityCheck();
        tpLog.sanityCheck();
    }

    @Test(expected = RuntimeException.class)
    public void testCheckWithException() throws IllegalAccessException, IOException {
        MappedFileQueue mfq = mock(MappedFileQueue.class);
        MappedFileQueue smfq = mock(MappedFileQueue.class);
        SparseConsumeQueue scq = mock(SparseConsumeQueue.class);
        doReturn(smfq).when(scq).getMappedFileQueue();
        CompactionLog.TopicPartitionLog tpLog = mock(CompactionLog.TopicPartitionLog.class);
        FieldUtils.writeField(tpLog, "mappedFileQueue", mfq, true);
        FieldUtils.writeField(tpLog, "consumeQueue", scq, true);

        Files.createDirectories(Paths.get(logPath, topic, String.valueOf(queueId)));
        Files.write(Paths.get(logPath, topic, String.valueOf(queueId), "102400"),
            RandomStringUtils.randomAlphanumeric(compactionFileSize).getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        MappedFile mappedFile = new DefaultMappedFile(
            Paths.get(logPath, topic, String.valueOf(queueId), "102400").toFile().getAbsolutePath(),
            compactionFileSize);
        doReturn(Lists.newArrayList(mappedFile)).when(mfq).getMappedFiles();
        doReturn(Lists.newArrayList()).when(smfq).getMappedFiles();

        doCallRealMethod().when(tpLog).sanityCheck();
        tpLog.sanityCheck();
    }

    @Test
    public void testCompaction() throws DigestException, NoSuchAlgorithmException, IllegalAccessException {
        Iterator<SelectMappedBufferResult> iterator = mock(Iterator.class);
        SelectMappedBufferResult smb = mock(SelectMappedBufferResult.class);
        when(iterator.hasNext()).thenAnswer((Answer<Boolean>)invocationOnMock -> queueOffset < 1024);
        when(iterator.next()).thenAnswer((Answer<SelectMappedBufferResult>)invocation ->
            new SelectMappedBufferResult(0, buildMessage(), 0, null));

        MappedFile mf = mock(MappedFile.class);
        List<MappedFile> mappedFileList = Lists.newArrayList(mf);
        doReturn(iterator).when(mf).iterator(0);

        MessageStore messageStore = mock(DefaultMessageStore.class);
        CommitLog commitLog = mock(CommitLog.class);
        when(messageStore.getCommitLog()).thenReturn(commitLog);
        when(commitLog.getCommitLogSize()).thenReturn(1024 * 1024);
        CompactionLog clog = mock(CompactionLog.class);
        FieldUtils.writeField(clog, "defaultMessageStore", messageStore, true);
        doCallRealMethod().when(clog).getOffsetMap(any());
        FieldUtils.writeField(clog, "positionMgr", positionMgr, true);

        queueOffset = 0;
        CompactionLog.OffsetMap offsetMap = clog.getOffsetMap(mappedFileList);
        assertEquals(1023, offsetMap.getLastOffset());

        doCallRealMethod().when(clog).compaction(any(List.class), any(CompactionLog.OffsetMap.class));
        doNothing().when(clog).putEndMessage(any(MappedFileQueue.class));
        doCallRealMethod().when(clog).checkAndPutMessage(any(SelectMappedBufferResult.class),
            any(MessageExt.class), any(CompactionLog.OffsetMap.class), any(CompactionLog.TopicPartitionLog.class));
        doCallRealMethod().when(clog).shouldRetainMsg(any(MessageExt.class), any(CompactionLog.OffsetMap.class));
        List<MessageExt> compactResult = Lists.newArrayList();
        when(clog.asyncPutMessage(any(ByteBuffer.class), any(MessageExt.class),
            any(CompactionLog.TopicPartitionLog.class)))
            .thenAnswer((Answer<CompletableFuture<PutMessageResult>>)invocation -> {
                compactResult.add(invocation.getArgument(1));
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK,
                    new AppendMessageResult(AppendMessageStatus.PUT_OK)));
            });
        queueOffset = 0;
        clog.compaction(mappedFileList, offsetMap);
        assertEquals(keyCount, compactResult.size());
        assertEquals(1014, compactResult.stream().mapToLong(MessageExt::getQueueOffset).min().orElse(1024));
        assertEquals(1023, compactResult.stream().mapToLong(MessageExt::getQueueOffset).max().orElse(0));
    }

    @Test
    public void testReplaceFiles() throws IOException, IllegalAccessException {
        Assume.assumeFalse(MixAll.isWindows());
        CompactionLog clog = mock(CompactionLog.class);
        doCallRealMethod().when(clog).replaceFiles(anyList(), any(CompactionLog.TopicPartitionLog.class),
            any(CompactionLog.TopicPartitionLog.class));
        doCallRealMethod().when(clog).replaceCqFiles(any(SparseConsumeQueue.class),
            any(SparseConsumeQueue.class), anyList());

        CompactionLog.TopicPartitionLog dest = mock(CompactionLog.TopicPartitionLog.class);
        MappedFileQueue destMFQ = mock(MappedFileQueue.class);
        when(dest.getLog()).thenReturn(destMFQ);
        List<MappedFile> destFiles = Lists.newArrayList();
        when(destMFQ.getMappedFiles()).thenReturn(destFiles);

        List<MappedFile> srcFiles = Lists.newArrayList();
        String fileName = logPath + File.separator + COMPACTING_SUB_FOLDER + File.separator + String.format("%010d", 0);
        MappedFile mf = new DefaultMappedFile(fileName, 1024);
        srcFiles.add(mf);
        MappedFileQueue srcMFQ = mock(MappedFileQueue.class);
        when(srcMFQ.getMappedFiles()).thenReturn(srcFiles);
        CompactionLog.TopicPartitionLog src = mock(CompactionLog.TopicPartitionLog.class);
        when(src.getLog()).thenReturn(srcMFQ);

        FieldUtils.writeField(clog, "readMessageLock", new PutMessageSpinLock(), true);

        clog.replaceFiles(Lists.newArrayList(), dest, src);
        assertEquals(destFiles.size(), 1);
        destFiles.forEach(f -> {
            assertFalse(f.getFileName().contains(COMPACTING_SUB_FOLDER));
        });
    }

}