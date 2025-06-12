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
package org.apache.rocketmq.tieredstore.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.Assert;
import org.junit.Test;

public class GroupCommitContextTest {

    @Test
    public void groupCommitContextTest() {
        GroupCommitContext releaseGroupCommitContext = new GroupCommitContext();
        releaseGroupCommitContext.release();

        long endOffset = 1000;
        List<DispatchRequest> dispatchRequestList = new ArrayList<>();
        dispatchRequestList.add(new DispatchRequest(1000));
        List<SelectMappedBufferResult> selectMappedBufferResultList = new ArrayList<>();
        selectMappedBufferResultList.add(new SelectMappedBufferResult(100, ByteBuffer.allocate(10), 1000, null));
        GroupCommitContext groupCommitContext = new GroupCommitContext();
        groupCommitContext.setEndOffset(endOffset);
        groupCommitContext.setBufferList(selectMappedBufferResultList);
        groupCommitContext.setDispatchRequests(dispatchRequestList);

        Assert.assertTrue(groupCommitContext.getEndOffset() == endOffset);
        Assert.assertTrue(groupCommitContext.getBufferList().equals(selectMappedBufferResultList));
        Assert.assertTrue(groupCommitContext.getDispatchRequests().equals(dispatchRequestList));
        groupCommitContext.release();
        Assert.assertTrue(groupCommitContext.getDispatchRequests() == null);
        Assert.assertTrue(groupCommitContext.getBufferList() == null);
        Assert.assertTrue(dispatchRequestList.isEmpty());
        Assert.assertTrue(selectMappedBufferResultList.isEmpty());
    }

}
