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
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.common.ReadClosure;
import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.entry.DLedgerEntryType;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class DLedgerLatestCommitLogTest extends MessageStoreTestBase {

    private static final int QUEUE_ID = 0;

    @Test
    public void testUncommittedTailIsNotReadable() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DefaultMessageStore leaderStore = null;
        try {
            leaderStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, "n0", false, 0);
            String topic = UUID.randomUUID().toString();
            MessageExtBrokerInner message = buildMessage();
            message.setTopic(topic);
            message.setQueueId(QUEUE_ID);

            PutMessageResult result = leaderStore.asyncPutMessage(message).get(5, SECONDS);
            Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, result.getPutMessageStatus());
            Assert.assertNotNull(result.getAppendMessageResult());
            Assert.assertTrue(result.getAppendMessageResult().getWroteOffset() > 0);

            DLedgerCommitLog commitLog = commitLog(leaderStore);
            Assert.assertEquals(-1, commitLog.getdLedgerServer().getMemberState().getCommittedIndex());
            Assert.assertEquals(-1, commitLog.getCommittedPos());
            Assert.assertEquals(0, commitLog.getMaxOffset());
            Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, QUEUE_ID));
            Assert.assertNull(commitLog.getData(0));
            Assert.assertFalse(commitLog.getData(0, 1, ByteBuffer.allocate(1)));
            Assert.assertNull(commitLog.getMessage(result.getAppendMessageResult().getWroteOffset(), 1));
        } finally {
            shutdownAndDestroy(leaderStore);
        }
    }

    @Test
    public void testSingleAndBatchAppendPositions() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(messageStore));
            String topic = UUID.randomUUID().toString();

            PutMessageResult singleResult = putSingle(messageStore, topic, 0);
            PutMessageResult singleMessageBatchResult = putBatch(messageStore, topic, 1, 1);
            PutMessageResult batchResult = putBatch(messageStore, topic, 3, 2);

            Assert.assertTrue(singleResult.getAppendMessageResult().getWroteOffset() > 0);
            Assert.assertTrue(singleMessageBatchResult.getAppendMessageResult().getWroteOffset()
                > singleResult.getAppendMessageResult().getWroteOffset());
            Assert.assertTrue(batchResult.getAppendMessageResult().getWroteOffset()
                > singleMessageBatchResult.getAppendMessageResult().getWroteOffset());
            Assert.assertEquals(1, singleMessageBatchResult.getAppendMessageResult().getMsgNum());
            Assert.assertEquals(3, batchResult.getAppendMessageResult().getMsgNum());
            Assert.assertNotNull(singleResult.getAppendMessageResult().getMsgId());
            Assert.assertNotNull(singleMessageBatchResult.getAppendMessageResult().getMsgId());
            Assert.assertNotNull(batchResult.getAppendMessageResult().getMsgId());
            Assert.assertEquals(1,
                singleMessageBatchResult.getAppendMessageResult().getMsgId().split(",").length);
            Assert.assertEquals(3, batchResult.getAppendMessageResult().getMsgId().split(",").length);
            awaitStoreReady(messageStore, topic, 5);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertTrue(commitLog(messageStore).getCommittedPos()
                > batchResult.getAppendMessageResult().getWroteOffset());
            doGetMessages(messageStore, topic, QUEUE_ID, 5, 0);
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testThreeNodeElectionAndFailover() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d",
            nextPort(), nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        List<DefaultMessageStore> allStores = new ArrayList<>();
        try {
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n0", peers, null, false, 0));
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n1", peers, null, false, 0));
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n2", peers, null, false, 0));
            List<DefaultMessageStore> activeStores = new ArrayList<>(allStores);
            DefaultMessageStore firstLeader = awaitLeader(activeStores);
            String topic = UUID.randomUUID().toString();

            putSingle(firstLeader, topic, 0);
            putSingle(firstLeader, topic, 1);
            putSingle(firstLeader, topic, 2);
            for (DefaultMessageStore store : activeStores) {
                awaitStoreReady(store, topic, 3);
            }
            long committedBeforeFailover = commitLog(firstLeader).getCommittedPos();

            firstLeader.shutdown();
            activeStores.remove(firstLeader);
            DefaultMessageStore secondLeader = awaitLeader(activeStores);
            Assert.assertNotSame(firstLeader, secondLeader);
            awaitStoreReady(secondLeader, topic, 3);
            Assert.assertTrue(commitLog(secondLeader).getCommittedPos() >= committedBeforeFailover);
            doGetMessages(secondLeader, topic, QUEUE_ID, 3, 0);

            // Broker-side DLedgerRoleChangeHandler does this before accepting writes on a new leader.
            secondLeader.recoverTopicQueueTable();
            putBatch(secondLeader, topic, 3, 3);
            for (DefaultMessageStore store : activeStores) {
                awaitStoreReady(store, topic, 6);
            }
            doGetMessages(secondLeader, topic, QUEUE_ID, 6, 0);
        } finally {
            for (DefaultMessageStore store : allStores) {
                shutdownAndDestroy(store);
            }
        }
    }

    @Test
    public void testRestartRecoversCommittedBoundaryBeforeNewWrite() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        DefaultMessageStore currentStore = null;
        try {
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            doPutMessages(currentStore, topic, QUEUE_ID, 10, 0);
            awaitStoreReady(currentStore, topic, 10);
            doGetMessages(currentStore, topic, QUEUE_ID, 10, 0);
            long physicalBeforeRestart = currentStore.getMaxPhyOffset();
            long maxCqOffsetBeforeRestart = currentStore.getMaxOffsetInQueue(topic, QUEUE_ID);
            List<byte[]> bodiesBeforeRestart = readMessageBodies(currentStore, topic, QUEUE_ID, 10);
            long committedIndexBeforeRestart = committedIndex(currentStore);
            Assert.assertTrue(physicalBeforeRestart > 0);
            Assert.assertEquals(10, maxCqOffsetBeforeRestart);
            Assert.assertTrue(committedIndexBeforeRestart >= 9);

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeRestart);
            assertNoopEntry(currentStore, committedIndexBeforeRestart + 1);
            awaitStoreReady(currentStore, topic, maxCqOffsetBeforeRestart);
            Assert.assertTrue(currentStore.getMaxPhyOffset() >= physicalBeforeRestart);
            Assert.assertEquals(0, currentStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertEquals(maxCqOffsetBeforeRestart,
                currentStore.getMaxOffsetInQueue(topic, QUEUE_ID));
            assertMessageBodies(currentStore, topic, QUEUE_ID, bodiesBeforeRestart);
            Assert.assertEquals(commitLog(currentStore).getCommittedPos(), currentStore.getCommitLog().getMaxOffset());
            doGetMessages(currentStore, topic, QUEUE_ID, 10, 0);

            putSingle(currentStore, topic, 10);
            awaitStoreReady(currentStore, topic, 11);
            doGetMessages(currentStore, topic, QUEUE_ID, 11, 0);
            long committedPosBeforeSecondRestart = commitLog(currentStore).getCommittedPos();
            long committedIndexBeforeSecondRestart = committedIndex(currentStore);

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeSecondRestart);
            assertNoopEntry(currentStore, committedIndexBeforeSecondRestart + 1);
            awaitStoreReady(currentStore, topic, 11);
            Assert.assertEquals(0, currentStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertTrue(commitLog(currentStore).getCommittedPos() >= committedPosBeforeSecondRestart);
            Assert.assertEquals(commitLog(currentStore).getCommittedPos(), currentStore.getCommitLog().getMaxOffset());
            doGetMessages(currentStore, topic, QUEUE_ID, 11, 0);
        } finally {
            shutdownAndDestroy(currentStore);
        }
    }

    @Test
    public void testNoopDispatchContractAndBounds() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            DLedgerCommitLog commitLog = commitLog(messageStore);

            ByteBuffer noopBuffer = ByteBuffer.allocate(DLedgerEntry.BODY_OFFSET);
            DLedgerEntryCoder.encode(new DLedgerEntry(DLedgerEntryType.NOOP), noopBuffer);
            DispatchRequest noop = commitLog.checkMessageAndReturnSize(noopBuffer, true, false, false);
            Assert.assertTrue(noop.isSuccess());
            Assert.assertEquals(0, noop.getMsgSize());
            Assert.assertEquals(DLedgerEntry.BODY_OFFSET, noop.getBufferSize());
            Assert.assertEquals(DLedgerEntry.BODY_OFFSET, noopBuffer.position());

            ByteBuffer undersized = noopHeader(DLedgerEntry.BODY_OFFSET - 1);
            DispatchRequest invalidSize = commitLog.checkMessageAndReturnSize(undersized, true, false, false);
            Assert.assertFalse(invalidSize.isSuccess());
            Assert.assertEquals(-1, invalidSize.getMsgSize());
            Assert.assertEquals(0, undersized.position());

            ByteBuffer truncated = noopHeader(DLedgerEntry.BODY_OFFSET + 1);
            DispatchRequest invalidBounds = commitLog.checkMessageAndReturnSize(truncated, true, false, false);
            Assert.assertFalse(invalidBounds.isSuccess());
            Assert.assertEquals(-1, invalidBounds.getMsgSize());
            Assert.assertEquals(0, truncated.position());

            awaitLeader(Arrays.asList(messageStore));
            String topic = UUID.randomUUID().toString();
            putSingle(messageStore, topic, 0);
            awaitStoreReady(messageStore, topic, 1);
            DLedgerServer server = commitLog.getdLedgerServer();
            DLedgerEntry normalEntry = server.getDLedgerStore().get(
                server.getDLedgerStore().getLedgerEndIndex());
            Assert.assertEquals(DLedgerEntryType.NORMAL.getMagic(), normalEntry.getMagic());
            byte[] innerMessage = normalEntry.getBody();

            ByteBuffer legalFollowingEntry = normalEntryBuffer(innerMessage, 0, null);
            byte[] legalFollowingBytes = new byte[legalFollowingEntry.remaining()];
            legalFollowingEntry.get(legalFollowingBytes);
            byte[] oversizedInnerMessage = Arrays.copyOf(innerMessage, innerMessage.length);
            ByteBuffer.wrap(oversizedInnerMessage).putInt(innerMessage.length + Integer.BYTES);
            ByteBuffer crossingEntry = normalEntryBuffer(
                oversizedInnerMessage, 0, legalFollowingBytes);
            DispatchRequest crossingRequest = commitLog.checkMessageAndReturnSize(
                crossingEntry, true, false, false);

            ByteBuffer mismatchedEntry = normalEntryBuffer(innerMessage, Integer.BYTES, null);
            DispatchRequest mismatchedRequest = commitLog.checkMessageAndReturnSize(
                mismatchedEntry, true, false, false);

            Assert.assertFalse(crossingRequest.isSuccess());
            Assert.assertFalse(mismatchedRequest.isSuccess());
            Assert.assertArrayEquals(new int[] {0, 0},
                new int[] {crossingEntry.position(), mismatchedEntry.position()});
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testRaftLogReadNoopDoesNotBuildConsumeQueue() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(messageStore));
            Assert.assertTrue(messageStore.getConsumeQueueTable().isEmpty());
            DLedgerServer server = commitLog(messageStore).getdLedgerServer();
            long previousCommittedIndex = committedIndex(messageStore);
            long previousLedgerEndIndex = server.getDLedgerStore().getLedgerEndIndex();

            Status status = appendRaftLogNoop(messageStore);
            Assert.assertTrue(status.isOk());
            awaitCommittedPast(messageStore, previousCommittedIndex);
            Assert.assertEquals(previousLedgerEndIndex + 1,
                server.getDLedgerStore().getLedgerEndIndex());
            assertNoopEntry(messageStore, previousLedgerEndIndex + 1);
            awaitNoopConsumed(messageStore);

            Assert.assertTrue(messageStore.getConsumeQueueTable().isEmpty());
            Assert.assertTrue(commitLog(messageStore).getCommittedPos() > 0);
            Assert.assertEquals(commitLog(messageStore).getCommittedPos(), messageStore.getCommitLog().getMaxOffset());
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testAbnormalRecoveryAcrossLeadingNoop() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = String.format("%s%s%s%s", UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), UUID.randomUUID());
        DefaultMessageStore currentStore = null;
        try {
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            Assert.assertTrue(appendRaftLogNoop(currentStore).isOk());
            awaitCommittedPast(currentStore, -1);
            assertNoopEntry(currentStore, 0);
            awaitNoopConsumed(currentStore);
            Assert.assertTrue(currentStore.getConsumeQueueTable().isEmpty());

            putSingle(currentStore, topic, 0);
            awaitStoreReady(currentStore, topic, 1);
            assertMessageMagic(currentStore, topic, QUEUE_ID,
                MessageDecoder.MESSAGE_MAGIC_CODE_V2);
            doGetMessages(currentStore, topic, QUEUE_ID, 1, 0);
            long committedIndexBeforeRestart = committedIndex(currentStore);
            long committedPosBeforeRestart = commitLog(currentStore).getCommittedPos();

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeRestart);
            assertNoopEntry(currentStore, 0);
            assertNoopEntry(currentStore, committedIndexBeforeRestart + 1);
            awaitStoreReady(currentStore, topic, 1);
            Assert.assertEquals(1, currentStore.getConsumeQueueTable().size());
            Assert.assertTrue(currentStore.getConsumeQueueTable().containsKey(topic));
            Assert.assertTrue(commitLog(currentStore).getCommittedPos() >= committedPosBeforeRestart);
            doGetMessages(currentStore, topic, QUEUE_ID, 1, 0);

            putSingle(currentStore, topic, 1);
            awaitStoreReady(currentStore, topic, 2);
            doGetMessages(currentStore, topic, QUEUE_ID, 2, 0);
        } finally {
            shutdownAndDestroy(currentStore);
        }
    }

    @Test
    public void testFixedSizeReadsRespectCommittedBoundary() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = null;
        DefaultMessageStore followerStore = null;
        try {
            leaderStore = createDledgerMessageStore(
                createBaseDir(), group, "n0", peers, "n0", false, 0);
            followerStore = createDledgerMessageStore(
                createBaseDir(), group, "n1", peers, "n0", false, 0);
            String topic = UUID.randomUUID().toString();
            DLedgerCommitLog leaderCommitLog = commitLog(leaderStore);
            DLedgerMmapFileStore dLedgerStore =
                (DLedgerMmapFileStore) leaderCommitLog.getdLedgerServer().getdLedgerStore();
            MmapFileList dataFileList = dLedgerStore.getDataFileList();

            int messageCount = 0;
            while (dataFileList.getMappedFiles().size() < 2 && messageCount < 16) {
                MessageExtBrokerInner message = buildMessage();
                message.setTopic(topic);
                message.setQueueId(QUEUE_ID);
                message.setBody(new byte[16 * 1024]);
                PutMessageResult result = leaderStore.asyncPutMessage(message).get(5, SECONDS);
                Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
                messageCount++;
            }
            Assert.assertEquals(2, dataFileList.getMappedFiles().size());
            awaitStoreReady(leaderStore, topic, messageCount);
            awaitStoreReady(followerStore, topic, messageCount);

            long selectedBase = dataFileList.getMappedFiles().get(1).getFileFromOffset();
            long committedPos = leaderCommitLog.getCommittedPos();
            Assert.assertTrue(committedPos > selectedBase);

            followerStore.shutdown();
            MessageExtBrokerInner uncommittedMessage = buildMessage();
            uncommittedMessage.setTopic(topic);
            uncommittedMessage.setQueueId(QUEUE_ID);
            PutMessageResult uncommittedResult = leaderStore.asyncPutMessage(uncommittedMessage).get(5, SECONDS);
            Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH,
                uncommittedResult.getPutMessageStatus());
            Assert.assertEquals(committedPos, leaderCommitLog.getCommittedPos());
            Assert.assertTrue(dataFileList.getMaxWrotePosition() > committedPos);
            Assert.assertEquals(2, dataFileList.getMappedFiles().size());

            ByteBuffer oversizedDestination = ByteBuffer.allocate(64);
            Assert.assertTrue(leaderCommitLog.getData(committedPos - 1, 1, oversizedDestination));
            Assert.assertEquals(1, oversizedDestination.position());
            Assert.assertEquals(64, oversizedDestination.limit());

            Assert.assertEquals(1, dataFileList.deleteExpiredFileByTime(0, 0, 0, true));
            Assert.assertEquals(1, dataFileList.getMappedFiles().size());
            long firstSurvivingBase = dataFileList.getFirstMappedFile().getFileFromOffset();
            Assert.assertEquals(selectedBase, firstSurvivingBase);
            Assert.assertTrue(firstSurvivingBase > 0);
            Assert.assertTrue(firstSurvivingBase < committedPos);
            Assert.assertSame(dataFileList.getFirstMappedFile(),
                dataFileList.findMappedFileByOffset(0, true));

            int crossSize = (int) (committedPos - firstSurvivingBase + 1);
            Assert.assertTrue(firstSurvivingBase + crossSize <= dataFileList.getMaxWrotePosition());
            ByteBuffer crossBoundaryDestination = ByteBuffer.allocate(crossSize);
            Assert.assertFalse(leaderCommitLog.getData(0, crossSize, crossBoundaryDestination));
            Assert.assertEquals(0, crossBoundaryDestination.position());
            Assert.assertNull(leaderCommitLog.getMessage(0, crossSize));
        } finally {
            shutdownAndDestroy(followerStore);
            shutdownAndDestroy(leaderStore);
        }
    }

    private PutMessageResult putSingle(DefaultMessageStore messageStore, String topic, long expectedLogicOffset)
        throws Exception {
        MessageExtBrokerInner message = buildMessage();
        message.setTopic(topic);
        message.setQueueId(QUEUE_ID);
        PutMessageResult result = messageStore.asyncPutMessage(message).get(5, SECONDS);
        Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        Assert.assertNotNull(result.getAppendMessageResult());
        Assert.assertEquals(expectedLogicOffset, result.getAppendMessageResult().getLogicsOffset());
        return result;
    }

    private PutMessageResult putBatch(DefaultMessageStore messageStore, String topic, int batchSize,
        long expectedLogicOffset) throws Exception {
        MessageExtBatch batch = buildBatchMessage(batchSize);
        batch.setTopic(topic);
        batch.setQueueId(QUEUE_ID);
        PutMessageResult result = messageStore.asyncPutMessages(batch).get(5, SECONDS);
        Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        Assert.assertNotNull(result.getAppendMessageResult());
        Assert.assertEquals(expectedLogicOffset, result.getAppendMessageResult().getLogicsOffset());
        return result;
    }

    private DefaultMessageStore awaitLeader(List<DefaultMessageStore> stores) {
        AtomicReference<DefaultMessageStore> leaderRef = new AtomicReference<>();
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
            DefaultMessageStore leader = null;
            for (DefaultMessageStore store : stores) {
                if (commitLog(store).getdLedgerServer().getMemberState().isLeader()) {
                    if (leader != null) {
                        return false;
                    }
                    leader = store;
                }
            }
            leaderRef.set(leader);
            return leader != null;
        });
        return leaderRef.get();
    }

    private void awaitStoreReady(DefaultMessageStore messageStore, String topic, long expectedMaxOffset) {
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(expectedMaxOffset, messageStore.getMaxOffsetInQueue(topic, QUEUE_ID));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        });
    }

    private void awaitCommittedPast(DefaultMessageStore messageStore, long previousCommittedIndex) {
        DLedgerServer server = commitLog(messageStore).getdLedgerServer();
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
            long committedIndex = server.getMemberState().getCommittedIndex();
            return committedIndex > previousCommittedIndex
                && committedIndex == server.getDLedgerStore().getLedgerEndIndex();
        });
    }

    private void awaitNoopConsumed(DefaultMessageStore messageStore) {
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(commitLog(messageStore).getCommittedPos(), messageStore.getCommitLog().getMaxOffset());
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        });
    }

    private List<byte[]> readMessageBodies(DefaultMessageStore messageStore, String topic, int queueId,
        int messageCount) {
        List<byte[]> bodies = new ArrayList<>(messageCount);
        for (int i = 0; i < messageCount; i++) {
            GetMessageResult result = messageStore.getMessage("group", topic, queueId, i, 1, null);
            Assert.assertNotNull(result);
            try {
                Assert.assertFalse(result.getMessageBufferList().isEmpty());
                MessageExt message = MessageDecoder.decode(result.getMessageBufferList().get(0));
                Assert.assertNotNull(message);
                Assert.assertEquals(i, message.getQueueOffset());
                bodies.add(Arrays.copyOf(message.getBody(), message.getBody().length));
            } finally {
                result.release();
            }
        }
        return bodies;
    }

    private void assertMessageBodies(DefaultMessageStore messageStore, String topic, int queueId,
        List<byte[]> expectedBodies) {
        List<byte[]> actualBodies = readMessageBodies(messageStore, topic, queueId, expectedBodies.size());
        for (int i = 0; i < expectedBodies.size(); i++) {
            Assert.assertArrayEquals(expectedBodies.get(i), actualBodies.get(i));
        }
    }

    private void assertMessageMagic(DefaultMessageStore messageStore, String topic, int queueId,
        int expectedMagic) {
        GetMessageResult result = messageStore.getMessage("group", topic, queueId, 0, 1, null);
        Assert.assertNotNull(result);
        try {
            Assert.assertFalse(result.getMessageBufferList().isEmpty());
            ByteBuffer messageBuffer = result.getMessageBufferList().get(0).duplicate();
            Assert.assertEquals(expectedMagic,
                messageBuffer.getInt(messageBuffer.position() + MessageDecoder.MESSAGE_MAGIC_CODE_POSITION));
        } finally {
            result.release();
        }
    }

    private Status appendRaftLogNoop(DefaultMessageStore messageStore) throws Exception {
        CompletableFuture<Status> result = new CompletableFuture<>();
        commitLog(messageStore).getdLedgerServer().handleRead(ReadMode.RAFT_LOG_READ, new ReadClosure() {
            @Override
            public void done(Status status) {
                result.complete(status);
            }
        });
        return result.get(5, SECONDS);
    }

    private ByteBuffer noopHeader(int entrySize) {
        ByteBuffer buffer = ByteBuffer.allocate(DLedgerEntry.BODY_OFFSET);
        buffer.putInt(DLedgerEntryType.NOOP.getMagic());
        buffer.putInt(entrySize);
        buffer.position(0);
        buffer.limit(DLedgerEntry.BODY_OFFSET);
        return buffer;
    }

    private ByteBuffer normalEntryBuffer(byte[] innerMessage, int bodyPadding, byte[] trailingBytes) {
        int entrySize = DLedgerEntry.BODY_OFFSET + innerMessage.length + bodyPadding;
        int trailingSize = trailingBytes == null ? 0 : trailingBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(entrySize + trailingSize);
        buffer.putInt(DLedgerEntryType.NORMAL.getMagic());
        buffer.putInt(entrySize);
        buffer.position(DLedgerEntry.BODY_OFFSET);
        buffer.put(innerMessage);
        buffer.position(entrySize);
        if (trailingBytes != null) {
            buffer.put(trailingBytes);
        }
        buffer.flip();
        return buffer;
    }

    private void assertNoopEntry(DefaultMessageStore messageStore, long index) {
        DLedgerServer server = commitLog(messageStore).getdLedgerServer();
        DLedgerEntry entry = server.getDLedgerStore().get(index);
        Assert.assertNotNull(entry);
        Assert.assertEquals(DLedgerEntryType.NOOP.getMagic(), entry.getMagic());
    }

    private long committedIndex(DefaultMessageStore messageStore) {
        return commitLog(messageStore).getdLedgerServer().getMemberState().getCommittedIndex();
    }

    private DLedgerCommitLog commitLog(DefaultMessageStore messageStore) {
        return (DLedgerCommitLog) messageStore.getCommitLog();
    }

    private void shutdownAndDestroy(DefaultMessageStore messageStore) {
        if (messageStore == null) {
            return;
        }
        try {
            if (!messageStore.isShutdown()) {
                messageStore.shutdown();
            }
        } finally {
            messageStore.destroy();
        }
    }
}
