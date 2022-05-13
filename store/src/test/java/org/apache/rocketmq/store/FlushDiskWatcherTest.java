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

package org.apache.rocketmq.store;

import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class FlushDiskWatcherTest {

    private final long timeoutMill = 5000;

    @Test
    public void testTimeout() throws Exception {
        FlushDiskWatcher flushDiskWatcher = new FlushDiskWatcher();
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();

        int count = 100;
        List<GroupCommitRequest> requestList = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            GroupCommitRequest groupCommitRequest =
                    new GroupCommitRequest(0, timeoutMill);
            requestList.add(groupCommitRequest);
            flushDiskWatcher.add(groupCommitRequest);
        }
        Thread.sleep(2 * timeoutMill);

        for (GroupCommitRequest request : requestList) {
            request.wakeupCustomer(PutMessageStatus.PUT_OK);
        }

        for (GroupCommitRequest request : requestList) {
            Assert.assertTrue(request.future().isDone());
            Assert.assertEquals(request.future().get(), PutMessageStatus.FLUSH_DISK_TIMEOUT);
        }
        Assert.assertEquals(flushDiskWatcher.queueSize(), 0);
        flushDiskWatcher.shutdown();
    }

    @Test
    public void testWatcher() throws Exception {
        FlushDiskWatcher flushDiskWatcher = new FlushDiskWatcher();
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();

        int count = 100;
        List<GroupCommitRequest> requestList = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            GroupCommitRequest groupCommitRequest =
                    new GroupCommitRequest(0, timeoutMill);
            requestList.add(groupCommitRequest);
            flushDiskWatcher.add(groupCommitRequest);
            groupCommitRequest.wakeupCustomer(PutMessageStatus.PUT_OK);
        }
        Thread.sleep((timeoutMill << 20) / 1000000);
        for (GroupCommitRequest request : requestList) {
            Assert.assertTrue(request.future().isDone());
            Assert.assertEquals(request.future().get(), PutMessageStatus.PUT_OK);
        }
        Assert.assertEquals(flushDiskWatcher.queueSize(), 0);
        flushDiskWatcher.shutdown();
    }


}
