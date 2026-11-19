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
package org.apache.rocketmq.broker.pop;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.PopAckConstants;
import org.junit.Assert;
import org.junit.Test;

public class PopConsumerLockServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    public void consumerLockTest() throws NoSuchFieldException, IllegalAccessException {
        String groupId = "groupId";
        String topicId = "topicId";

        PopConsumerLockService lockService =
            new PopConsumerLockService(TimeUnit.MINUTES.toMillis(2));

        Assert.assertTrue(lockService.tryLock(groupId, topicId));
        Assert.assertFalse(lockService.tryLock(groupId, topicId));
        lockService.unlock(groupId, topicId);

        Assert.assertTrue(lockService.tryLock(groupId, topicId));
        Assert.assertFalse(lockService.tryLock(groupId, topicId));
        Assert.assertFalse(lockService.isLockTimeout(groupId, topicId));
        lockService.removeTimeout();

        // set expired
        Field field = PopConsumerLockService.class.getDeclaredField("lockTable");
        field.setAccessible(true);
        Map<String, PopConsumerLockService.TimedLock> table =
            (Map<String, PopConsumerLockService.TimedLock>) field.get(lockService);

        Field lockTime = PopConsumerLockService.TimedLock.class.getDeclaredField("lockTime");
        lockTime.setAccessible(true);
        lockTime.set(table.get(groupId + PopAckConstants.SPLIT + topicId),
            System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(3));
        lockService.removeTimeout();

        Assert.assertEquals(0, table.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void removeTimeoutShouldNotRemoveReacquiredLock() throws Exception {
        String key = "groupId" + PopAckConstants.SPLIT + "topicId";
        PopConsumerLockService lockService =
            new PopConsumerLockService(TimeUnit.MINUTES.toMillis(2));

        Field tableField = PopConsumerLockService.class.getDeclaredField("lockTable");
        tableField.setAccessible(true);
        Map<String, PopConsumerLockService.TimedLock> table =
            (Map<String, PopConsumerLockService.TimedLock>) tableField.get(lockService);

        CountDownLatch timeoutObserved = new CountDownLatch(1);
        CountDownLatch continueCleanup = new CountDownLatch(1);
        PopConsumerLockService.TimedLock expiredLock = new PopConsumerLockService.TimedLock() {
            @Override
            public long getLockTime() {
                long observedLockTime = super.getLockTime();
                timeoutObserved.countDown();
                try {
                    if (!continueCleanup.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to continue lock cleanup");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return observedLockTime;
            }
        };

        Field lockTimeField = PopConsumerLockService.TimedLock.class.getDeclaredField("lockTime");
        lockTimeField.setAccessible(true);
        lockTimeField.setLong(expiredLock, 0L);
        table.put(key, expiredLock);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> cleanup = executor.submit(lockService::removeTimeout);
            Assert.assertTrue("Cleanup did not inspect the expired lock",
                timeoutObserved.await(5, TimeUnit.SECONDS));

            Assert.assertTrue("The expired lock should be reacquired before cleanup continues",
                lockService.tryLock(key));
            continueCleanup.countDown();
            cleanup.get(5, TimeUnit.SECONDS);

            Assert.assertFalse("Cleanup removed the reacquired lock and allowed a second holder",
                lockService.tryLock(key));
        } finally {
            continueCleanup.countDown();
            executor.shutdownNow();
        }
    }
}
