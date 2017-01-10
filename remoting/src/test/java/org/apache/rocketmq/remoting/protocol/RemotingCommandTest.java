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
package org.apache.rocketmq.remoting.protocol;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.junit.Assert;
import org.junit.Test;

public class RemotingCommandTest {
    @Test
    public void testCreateRequestCommand() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        Assert.assertEquals(code, cmd.getCode());
        Assert.assertEquals(2333, cmd.getVersion());
        Assert.assertEquals(0, cmd.getFlag() & 0x01); //flag bit 0: 0 presents request
    }

    @Test
    public void testCreateResponseCommandSuccess() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code ,remark, SampleCommandCustomHeader.class);
        Assert.assertEquals(code, cmd.getCode());
        Assert.assertEquals(2333, cmd.getVersion());
        Assert.assertEquals(remark, cmd.getRemark());
        Assert.assertEquals(1, cmd.getFlag() & 0x01); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommandWithNoHeader() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code ,remark);
        Assert.assertEquals(code, cmd.getCode());
        Assert.assertEquals(2333, cmd.getVersion());
        Assert.assertEquals(remark, cmd.getRemark());
        Assert.assertEquals(1, cmd.getFlag() & 0x01); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommandNull() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code ,remark, CommandCustomHeader.class);
        Assert.assertNull(cmd);
    }

    @Test
    public void testCreateResponseCommandError() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        RemotingCommand cmd = RemotingCommand.createResponseCommand(SampleCommandCustomHeader.class);
        Assert.assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, cmd.getCode());
        Assert.assertEquals(2333, cmd.getVersion());
        Assert.assertEquals("not set any response code", cmd.getRemark());
        Assert.assertEquals(1, cmd.getFlag() & 0x01); //flag bit 0: 1 presents response
    }

    @Test
    public void testMarkProtocolTypeForJSON() {
        int source = 261;
        SerializeType type = SerializeType.JSON;
        byte[] result = RemotingCommand.markProtocolType(source, type);
        Assert.assertArrayEquals(new byte[]{0, 0, 1, 5}, result);
    }

    @Test
    public void testMarkProtocolTypeForROCKETMQ() {
        int source = 16777215;
        SerializeType type = SerializeType.ROCKETMQ;
        byte[] result = RemotingCommand.markProtocolType(source, type);
        Assert.assertArrayEquals(new byte[]{1, -1, -1, -1}, result);
    }

}

class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
        return;
    }
}