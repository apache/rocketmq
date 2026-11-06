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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Status;
import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ResponseWriterTest {

    @Test
    public void testSummarizeResponseDoesNotExposeMessagePayload() {
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder()
            .setMessage(Message.newBuilder().setBody(ByteString.copyFromUtf8("secret-payload")).build())
            .build();

        String summary = ResponseWriter.summarizeResponse(response);

        assertThat(summary).contains("ReceiveMessageResponse");
        assertThat(summary).doesNotContain("secret-payload");
        assertThat(summary).doesNotContain("body");
    }

    @Test
    public void testSummarizeResponseIncludesStatusCode() {
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder()
            .setStatus(Status.newBuilder().setCode(Code.OK).setMessage("OK").build())
            .build();

        String summary = ResponseWriter.summarizeResponse(response);

        assertThat(summary).contains("ReceiveMessageResponse");
        assertThat(summary).contains("statusCode=OK");
    }
}
