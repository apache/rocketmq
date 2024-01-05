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
package org.apache.rocketmq.auth.authentication.builder;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultAuthenticationContextBuilderTest {

    private DefaultAuthenticationContextBuilder builder;

    @Before
    public void setUp() throws Exception {
        builder = new DefaultAuthenticationContextBuilder();
    }

    @Test
    public void build1() {
        Resource topic = Resource.newBuilder().setName("topic-test").build();
        {
            SendMessageRequest request = SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder().setTopic(topic)
                    .setBody(ByteString.copyFromUtf8("message-body"))
                    .build())
                .build();
            Metadata metadata = new Metadata();
            metadata.put(GrpcConstants.AUTHORIZATION, "MQv2-HMAC-SHA1 Credential=abc, SignedHeaders=x-mq-date-time, Signature=D18A9CBCDDBA9041D6693268FEF15A989E64430B");
            metadata.put(GrpcConstants.DATE_TIME, "20231227T194619Z");
            DefaultAuthenticationContext context = builder.build(metadata, request);
            Assert.assertNotNull(context);
            Assert.assertEquals("abc", context.getUsername());
            Assert.assertEquals("0YqcvN26kEHWaTJo/vFamJ5kQws=", context.getSignature());
            Assert.assertEquals("20231227T194619Z", new String(context.getContent(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void build2() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setTopic("topic-test");
        requestHeader.setQueueId(0);
        requestHeader.setBornTimestamp(117036786441330L);
        requestHeader.setBname("brokerName-1");
        RemotingCommand request = RemotingCommand.createRequestCommand(10, requestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "abc");
        request.addExtField("Signature", "ZG26exJ5u9q1fwZlO4DCmz2Rs88=");
        request.makeCustomHeaderToNet();
        DefaultAuthenticationContext context = builder.build(context, request);
        Assert.assertNotNull(context);
        Assert.assertEquals("abc", context.getUsername());
        Assert.assertEquals("ZG26exJ5u9q1fwZlO4DCmz2Rs88=", context.getSignature());
        Assert.assertEquals("abcfalsebrokerName-11170367864413300topic-testfalse", new String(context.getContent(), StandardCharsets.UTF_8));
    }
}