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
package org.apache.rocketmq.broker.longpolling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LmqPullRequestHoldServiceTest {
    private static final String TOPIC = MixAll.LMQ_PREFIX + "cleanup-race";
    private static final int QUEUE_ID = 0;
    private static final String KEY = TOPIC + "@" + QUEUE_ID;

    @Test
    public void testConcurrentSuspendRemainsReachableDuringEmptyBucketCleanup() throws Exception {
        BrokerController brokerController = mock(BrokerController.class);
        MessageStore messageStore = mock(MessageStore.class);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(messageStore.getMaxOffsetInQueue(TOPIC, QUEUE_ID)).thenReturn(0L);

        BlockingCleanupMap pullRequestTable = new BlockingCleanupMap(KEY);
        pullRequestTable.put(KEY, new ManyPullRequest());
        LmqPullRequestHoldService service = new LmqPullRequestHoldService(brokerController);
        service.pullRequestTable = pullRequestTable;

        PullRequest pullRequest = mock(PullRequest.class);
        when(pullRequest.getRequestCommand()).thenReturn(mock(RemotingCommand.class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> suspendFuture = executor.submit(() -> {
            try {
                if (!pullRequestTable.cleanupEntered.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("cleanup did not reach the remove point");
                }
                service.suspendPullRequest(TOPIC, QUEUE_ID, pullRequest);
                ManyPullRequest mappedBucket = service.pullRequestTable.get(KEY);
                assertNotNull("suspend should keep a reachable bucket before cleanup", mappedBucket);
                assertTrue("suspend should add the request before cleanup continues",
                    mappedBucket.getPullRequestList().contains(pullRequest));
            } finally {
                pullRequestTable.allowCleanup.countDown();
            }
            return null;
        });

        try {
            service.checkHoldRequest();
            suspendFuture.get(5, TimeUnit.SECONDS);

            ManyPullRequest mappedBucket = service.pullRequestTable.get(KEY);
            assertNotNull("concurrently suspended request should retain a reachable bucket", mappedBucket);
            assertTrue("concurrently suspended request should remain reachable",
                mappedBucket.getPullRequestList().contains(pullRequest));
        } finally {
            pullRequestTable.allowCleanup.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSuspendDoesNotAppendToDetachedBucket() throws Exception {
        BrokerController brokerController = mock(BrokerController.class);
        BlockingLookupMap pullRequestTable = new BlockingLookupMap(KEY);
        pullRequestTable.put(KEY, new ManyPullRequest());
        LmqPullRequestHoldService service = new LmqPullRequestHoldService(brokerController);
        service.pullRequestTable = pullRequestTable;

        PullRequest pullRequest = mock(PullRequest.class);
        when(pullRequest.getRequestCommand()).thenReturn(mock(RemotingCommand.class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> suspendFuture = executor.submit(
            () -> service.suspendPullRequest(TOPIC, QUEUE_ID, pullRequest));

        try {
            assertTrue("suspend should select the mapped bucket",
                pullRequestTable.bucketSelected.await(5, TimeUnit.SECONDS));
            pullRequestTable.computeIfPresent(KEY, (key, current) -> current.isEmpty() ? null : current);
            pullRequestTable.allowAppend.countDown();
            suspendFuture.get(5, TimeUnit.SECONDS);

            ManyPullRequest mappedBucket = pullRequestTable.get(KEY);
            assertNotNull("suspended request should retain a reachable bucket", mappedBucket);
            assertTrue("suspended request should remain reachable from the table",
                mappedBucket.getPullRequestList().contains(pullRequest));
        } finally {
            pullRequestTable.allowAppend.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testReplayRemainsReachableAfterConcurrentEmptyBucketCleanup() throws Exception {
        BrokerController brokerController = mock(BrokerController.class);
        MessageStore messageStore = mock(MessageStore.class);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(messageStore.getMaxOffsetInQueue(TOPIC, QUEUE_ID)).thenReturn(0L);

        LmqPullRequestHoldService service = new LmqPullRequestHoldService(brokerController);
        CountDownLatch replayEvaluationStarted = new CountDownLatch(1);
        CountDownLatch allowReplay = new CountDownLatch(1);
        PullRequest pullRequest = mock(PullRequest.class);
        when(pullRequest.getRequestCommand()).thenReturn(mock(RemotingCommand.class));
        when(pullRequest.getPullFromThisOffset()).thenAnswer(invocation -> {
            replayEvaluationStarted.countDown();
            if (!allowReplay.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting to replay the pull request");
            }
            return 0L;
        });
        when(pullRequest.getSuspendTimestamp()).thenReturn(System.currentTimeMillis());
        when(pullRequest.getTimeoutMillis()).thenReturn(TimeUnit.HOURS.toMillis(1));
        service.suspendPullRequest(TOPIC, QUEUE_ID, pullRequest);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> notificationFuture = executor.submit(
            () -> service.notifyMessageArriving(TOPIC, QUEUE_ID, 0L));

        try {
            assertTrue("notification should start evaluating the detached request",
                replayEvaluationStarted.await(5, TimeUnit.SECONDS));
            assertTrue("notification should have cleared the mapped bucket before evaluation",
                service.pullRequestTable.get(KEY).isEmpty());

            service.checkHoldRequest();
            assertFalse("cleanup should remove the empty mapped bucket",
                service.pullRequestTable.containsKey(KEY));

            allowReplay.countDown();
            notificationFuture.get(5, TimeUnit.SECONDS);

            ManyPullRequest replayBucket = service.pullRequestTable.get(KEY);
            assertNotNull("replayed request should restore a reachable bucket", replayBucket);
            assertTrue("replayed request should remain reachable from the table",
                replayBucket.getPullRequestList().contains(pullRequest));
        } finally {
            allowReplay.countDown();
            executor.shutdownNow();
        }
    }

    private static class BlockingLookupMap extends ConcurrentHashMap<String, ManyPullRequest> {
        private final String blockedKey;
        private final CountDownLatch bucketSelected = new CountDownLatch(1);
        private final CountDownLatch allowAppend = new CountDownLatch(1);

        BlockingLookupMap(String blockedKey) {
            this.blockedKey = blockedKey;
        }

        @Override
        public ManyPullRequest get(Object key) {
            ManyPullRequest current = super.get(key);
            if (blockedKey.equals(key) && current != null) {
                bucketSelected.countDown();
                try {
                    if (!allowAppend.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to append to the selected bucket");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return current;
        }

        @Override
        public ManyPullRequest compute(String key,
            BiFunction<? super String, ? super ManyPullRequest, ? extends ManyPullRequest> remappingFunction) {
            ManyPullRequest result = super.compute(key, remappingFunction);
            if (blockedKey.equals(key)) {
                bucketSelected.countDown();
            }
            return result;
        }
    }

    private static class BlockingCleanupMap extends ConcurrentHashMap<String, ManyPullRequest> {
        private final String blockedKey;
        private final CountDownLatch cleanupEntered = new CountDownLatch(1);
        private final CountDownLatch allowCleanup = new CountDownLatch(1);

        BlockingCleanupMap(String blockedKey) {
            this.blockedKey = blockedKey;
        }

        @Override
        public ManyPullRequest remove(Object key) {
            if (blockedKey.equals(key)) {
                awaitCleanup();
            }
            return super.remove(key);
        }

        @Override
        public ManyPullRequest computeIfPresent(String key,
            BiFunction<? super String, ? super ManyPullRequest, ? extends ManyPullRequest> remappingFunction) {
            if (blockedKey.equals(key)) {
                awaitCleanup();
            }
            return super.computeIfPresent(key, remappingFunction);
        }

        private void awaitCleanup() {
            cleanupEntered.countDown();
            try {
                if (!allowCleanup.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to clean up the empty bucket");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
