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

import java.util.List;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class GroupCommitContext {

    private long endOffset;

    private List<SelectMappedBufferResult> bufferList;

    private List<DispatchRequest> dispatchRequests;

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public List<SelectMappedBufferResult> getBufferList() {
        return bufferList;
    }

    public void setBufferList(List<SelectMappedBufferResult> bufferList) {
        this.bufferList = bufferList;
    }

    public List<DispatchRequest> getDispatchRequests() {
        return dispatchRequests;
    }

    public void setDispatchRequests(List<DispatchRequest> dispatchRequests) {
        this.dispatchRequests = dispatchRequests;
    }

    public void release() {
        if (bufferList != null) {
            for (SelectMappedBufferResult bufferResult : bufferList) {
                bufferResult.release();
            }
            bufferList.clear();
            bufferList = null;
        }
        if (dispatchRequests != null) {
            dispatchRequests.clear();
            dispatchRequests = null;
        }

    }
}
