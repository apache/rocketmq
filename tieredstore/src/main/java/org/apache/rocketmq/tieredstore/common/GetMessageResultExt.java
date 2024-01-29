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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class GetMessageResultExt extends GetMessageResult {

    private final List<Long> tagCodeList;

    public GetMessageResultExt() {
        this.tagCodeList = new ArrayList<>();
    }

    public void addMessageExt(SelectMappedBufferResult bufferResult, long queueOffset, long tagCode) {
        super.addMessage(bufferResult, queueOffset);
        this.tagCodeList.add(tagCode);
    }

    public List<Long> getTagCodeList() {
        return tagCodeList;
    }

    public GetMessageResult doFilterMessage(MessageFilter messageFilter) {
        if (GetMessageStatus.FOUND != super.getStatus() || messageFilter == null) {
            return this;
        }

        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        result.setMinOffset(this.getMinOffset());
        result.setMaxOffset(this.getMaxOffset());
        result.setNextBeginOffset(this.getNextBeginOffset());

        for (int i = 0; i < this.getMessageMapedList().size(); i++) {
            if (!messageFilter.isMatchedByConsumeQueue(this.tagCodeList.get(i), null)) {
                continue;
            }

            SelectMappedBufferResult bufferResult = this.getMessageMapedList().get(i);
            if (!messageFilter.isMatchedByCommitLog(bufferResult.getByteBuffer().slice(), null)) {
                continue;
            }

            long offset = this.getMessageQueueOffset().get(i);
            result.addMessage(new SelectMappedBufferResult(bufferResult.getStartOffset(),
                bufferResult.getByteBuffer().asReadOnlyBuffer(), bufferResult.getSize(), null), offset);
        }

        if (result.getBufferTotalSize() == 0) {
            result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
        }
        return result;
    }
}
