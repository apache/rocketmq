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
package org.apache.rocketmq.acl.plain;

import java.nio.ByteBuffer;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlainAccessValidatorTest {

    PlainAccessValidator plainAccessValidator;

    @Before
    public void init() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        plainAccessValidator = new PlainAccessValidator();
    }

    @Test
    public void contentTest() {
        SessionCredentials sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("12345678");
        sessionCredentials.setSecurityToken("87654321");
        AclClientRPCHook aclClient = new AclClientRPCHook(sessionCredentials);

        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "127.0.0.1");
        String signature = AclUtils.calSignature(accessResource.getContent(), sessionCredentials.getSecretKey());

        Assert.assertEquals(accessResource.getSignature(), signature);

    }

    @Test
    public void validateTest() {
        SessionCredentials sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("12345678");
        sessionCredentials.setSecurityToken("87654321");
        AclClientRPCHook aclClient = new AclClientRPCHook(sessionCredentials);

        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
        plainAccessValidator.validate(accessResource);
    }
}
