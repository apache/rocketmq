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
}