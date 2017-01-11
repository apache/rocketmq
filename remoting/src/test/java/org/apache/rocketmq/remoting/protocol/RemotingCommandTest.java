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

import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
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

    @Test
    public void testEncodeAndDecodeWithEmptyBody() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);

        Assert.assertEquals(SerializeType.JSON, decodedCommand.getSerializeTypeCurrentRPC());
        Assert.assertNull(decodedCommand.getBody());
    }

    @Test
    public void testEncodeAndDecode() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        cmd.setBody(new byte[] { 0, 1, 2, 3, 4});

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);

        Assert.assertEquals(SerializeType.JSON, decodedCommand.getSerializeTypeCurrentRPC());
        Assert.assertArrayEquals(new byte[]{ 0, 1, 2, 3, 4}, decodedCommand.getBody());
    }

    @Test
    public void testEncodeAndDecodeWithExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new ExtFieldsHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        cmd.addExtField("key", "value");

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = RemotingCommand.decode(buffer);

        Assert.assertEquals("bilibili", decodedCommand.getExtFields().get("stringValue"));
        Assert.assertEquals("2333", decodedCommand.getExtFields().get("intValue"));
        Assert.assertEquals("23333333", decodedCommand.getExtFields().get("longValue"));
        Assert.assertEquals("true", decodedCommand.getExtFields().get("booleanValue"));
        Assert.assertEquals("0.618", decodedCommand.getExtFields().get("doubleValue"));

        Assert.assertEquals("value", decodedCommand.getExtFields().get("key"));

        try {
            CommandCustomHeader decodedHeader = decodedCommand.decodeCommandCustomHeader(ExtFieldsHeader.class);
            Assert.assertEquals("bilibili", ((ExtFieldsHeader)decodedHeader).getStringValue());
            Assert.assertEquals(2333, ((ExtFieldsHeader)decodedHeader).getIntValue());
            Assert.assertEquals(23333333l, ((ExtFieldsHeader)decodedHeader).getLongValue());
            Assert.assertEquals(true, ((ExtFieldsHeader)decodedHeader).isBooleanValue());
            Assert.assertEquals(true, ((ExtFieldsHeader)decodedHeader).getDoubleValue() - 0.618 < 0.01);

        } catch (RemotingCommandException ex) {
            Assert.fail();
        }
    }
}

class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
        return;
    }
}

class ExtFieldsHeader implements CommandCustomHeader {
    private String stringValue = "bilibili";
    private int intValue = 2333;
    private long longValue = 23333333l;
    private boolean booleanValue = true;
    private double doubleValue = 0.618;

    @Override
    public void checkFields() throws RemotingCommandException {
        return;
    }

    public String getStringValue() {
        return stringValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public boolean isBooleanValue() {
        return booleanValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }
}