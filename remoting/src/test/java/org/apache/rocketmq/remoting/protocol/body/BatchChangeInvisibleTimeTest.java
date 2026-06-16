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

package org.apache.rocketmq.remoting.protocol.body;

import java.util.Arrays;
import java.util.Collections;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchChangeInvisibleTimeTest {

    @Test
    public void testBatchChangeInvisibleTimeRequestBody() {
        BatchChangeInvisibleTimeRequestBody requestBody = new BatchChangeInvisibleTimeRequestBody();
        requestBody.setEntries(Arrays.asList(buildRequestEntry(1), buildRequestEntry(2)));

        BatchChangeInvisibleTimeRequestBody decoded =
            BatchChangeInvisibleTimeRequestBody.decode(requestBody.encode(), BatchChangeInvisibleTimeRequestBody.class);

        assertThat(decoded.getEntries()).hasSize(2);
        assertThat(decoded.getEntries().get(0).getConsumerGroup()).isEqualTo("group");
        assertThat(decoded.getEntries().get(0).getTopic()).isEqualTo("topic");
        assertThat(decoded.getEntries().get(0).getQueueId()).isEqualTo(1);
        assertThat(decoded.getEntries().get(0).getExtraInfo()).isEqualTo("0 100 1000 0 broker 1 10");
        assertThat(decoded.getEntries().get(0).getOffset()).isEqualTo(10);
        assertThat(decoded.getEntries().get(0).getInvisibleTime()).isEqualTo(3000);
        assertThat(decoded.getEntries().get(0).getLiteTopic()).isEqualTo("lite");
        assertThat(decoded.getEntries().get(0).isSuspend()).isTrue();
    }

    @Test
    public void testEmptyBatchChangeInvisibleTimeRequestBody() {
        BatchChangeInvisibleTimeRequestBody requestBody = new BatchChangeInvisibleTimeRequestBody();
        requestBody.setEntries(Collections.emptyList());

        BatchChangeInvisibleTimeRequestBody decoded =
            BatchChangeInvisibleTimeRequestBody.decode(requestBody.encode(), BatchChangeInvisibleTimeRequestBody.class);

        assertThat(decoded.getEntries()).isEmpty();
    }

    @Test
    public void testBatchChangeInvisibleTimeResponseBody() {
        BatchChangeInvisibleTimeResponseBody responseBody = new BatchChangeInvisibleTimeResponseBody();
        ChangeInvisibleTimeResponseEntry successEntry = new ChangeInvisibleTimeResponseEntry();
        successEntry.setCode(ResponseCode.SUCCESS);
        successEntry.setPopTime(200);
        successEntry.setInvisibleTime(3000);
        successEntry.setReviveQid(1);
        ChangeInvisibleTimeResponseEntry failedEntry = new ChangeInvisibleTimeResponseEntry();
        failedEntry.setCode(ResponseCode.NO_MESSAGE);
        responseBody.setEntries(Arrays.asList(successEntry, failedEntry));

        BatchChangeInvisibleTimeResponseBody decoded =
            BatchChangeInvisibleTimeResponseBody.decode(responseBody.encode(), BatchChangeInvisibleTimeResponseBody.class);

        assertThat(decoded.getEntries()).hasSize(2);
        assertThat(decoded.getEntries().get(0).getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(decoded.getEntries().get(0).getPopTime()).isEqualTo(200);
        assertThat(decoded.getEntries().get(0).getInvisibleTime()).isEqualTo(3000);
        assertThat(decoded.getEntries().get(0).getReviveQid()).isEqualTo(1);
        assertThat(decoded.getEntries().get(1).getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
    }

    private ChangeInvisibleTimeRequestEntry buildRequestEntry(int queueId) {
        ChangeInvisibleTimeRequestEntry entry = new ChangeInvisibleTimeRequestEntry();
        entry.setConsumerGroup("group");
        entry.setTopic("topic");
        entry.setQueueId(queueId);
        entry.setExtraInfo("0 100 1000 0 broker " + queueId + " 10");
        entry.setOffset(10);
        entry.setInvisibleTime(3000);
        entry.setLiteTopic("lite");
        entry.setSuspend(true);
        return entry;
    }
}
