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
package org.apache.rocketmq.acl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.acl.plain.AclTestHelper;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemotingClientAccessTest {

    private PlainAccessValidator plainAccessValidator;
    private AclClientRPCHook aclClient;
    private SessionCredentials sessionCredentials;

    private File confHome;

    private String clientAddress = "10.7.1.3";

    @Before
    public void init() throws IOException {
        String folder = "access_acl_conf";
        confHome = AclTestHelper.copyResources(folder, true);
        System.setProperty("rocketmq.home.dir", confHome.getAbsolutePath());
        System.setProperty("rocketmq.acl.plain.file", "/access_acl_conf/acl/plain_acl.yml".replace("/", File.separator));

        plainAccessValidator = new PlainAccessValidator();
        sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("rocketmq3");
        sessionCredentials.setSecretKey("12345678");
        aclClient = new AclClientRPCHook(sessionCredentials);
    }

    @After
    public void cleanUp() {
        AclTestHelper.recursiveDelete(confHome);
    }

    @Test(expected = AclException.class)
    public void testProduceDenyTopic() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicD");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest(clientAddress, remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), clientAddress);
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testProduceAuthorizedTopic() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest(clientAddress, remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), clientAddress);
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }


    @Test(expected = AclException.class)
    public void testConsumeDenyTopic() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicD");
        pullMessageRequestHeader.setConsumerGroup("groupB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "123.4.5.6");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }

    }

    @Test
    public void testConsumeAuthorizedTopic() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicB");
        pullMessageRequestHeader.setConsumerGroup("groupB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "123.4.5.6");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

    @Test(expected = AclException.class)
    public void testConsumeInDeniedGroup() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicB");
        pullMessageRequestHeader.setConsumerGroup("groupD");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "123.4.5.6");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testConsumeInAuthorizedGroup() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicB");
        pullMessageRequestHeader.setConsumerGroup("groupB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "123.4.5.6");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

}
