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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingCommandTest {
    @Test
    public void testMarkProtocolType_JSONProtocolType() {
        int source = 261;
        SerializeType type = SerializeType.JSON;

        byte[] result = new byte[4];
        int x = RemotingCommand.markProtocolType(source, type);
        result[0] = (byte) (x >> 24);
        result[1] = (byte) (x >> 16);
        result[2] = (byte) (x >> 8);
        result[3] = (byte) x;
        assertThat(result).isEqualTo(new byte[] {0, 0, 1, 5});
    }

    @Test
    public void testMarkProtocolType_ROCKETMQProtocolType() {
        int source = 16777215;
        SerializeType type = SerializeType.ROCKETMQ;
        byte[] result = new byte[4];
        int x = RemotingCommand.markProtocolType(source, type);
        result[0] = (byte) (x >> 24);
        result[1] = (byte) (x >> 16);
        result[2] = (byte) (x >> 8);
        result[3] = (byte) x;
        assertThat(result).isEqualTo(new byte[] {1, -1, -1, -1});
    }

    @Test
    public void testCreateRequestCommand_RegisterBroker() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        assertThat(cmd.getCode()).isEqualTo(code);
        assertThat(cmd.getVersion()).isEqualTo(2333);
        assertThat(cmd.getFlag() & 0x01).isEqualTo(0); //flag bit 0: 0 presents request
    }

    @Test
    public void testCreateResponseCommand_SuccessWithHeader() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, SampleCommandCustomHeader.class);
        assertThat(cmd.getCode()).isEqualTo(code);
        assertThat(cmd.getVersion()).isEqualTo(2333);
        assertThat(cmd.getRemark()).isEqualTo(remark);
        assertThat(cmd.getFlag() & 0x01).isEqualTo(1); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommand_SuccessWithoutHeader() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark);
        assertThat(cmd.getCode()).isEqualTo(code);
        assertThat(cmd.getVersion()).isEqualTo(2333);
        assertThat(cmd.getRemark()).isEqualTo(remark);
        assertThat(cmd.getFlag() & 0x01).isEqualTo(1); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommand_FailToCreateCommand() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, CommandCustomHeader.class);
        assertThat(cmd).isNull();
    }

    @Test
    public void testCreateResponseCommand_SystemError() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        RemotingCommand cmd = RemotingCommand.createResponseCommand(SampleCommandCustomHeader.class);
        assertThat(cmd.getCode()).isEqualTo(RemotingSysResponseCode.SYSTEM_ERROR);
        assertThat(cmd.getVersion()).isEqualTo(2333);
        assertThat(cmd.getRemark()).contains("not set any response code");
        assertThat(cmd.getFlag() & 0x01).isEqualTo(1); //flag bit 0: 1 presents response
    }

    @Test
    public void testEncodeAndDecode_EmptyBody() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            assertThat(decodedCommand.getSerializeTypeCurrentRPC()).isEqualTo(SerializeType.JSON);
            assertThat(decodedCommand.getBody()).isNull();
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }

    }

    @Test
    public void testEncodeAndDecode_FilledBody() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        cmd.setBody(new byte[] {0, 1, 2, 3, 4});

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            assertThat(decodedCommand.getSerializeTypeCurrentRPC()).isEqualTo(SerializeType.JSON);
            assertThat(decodedCommand.getBody()).isEqualTo(new byte[] {0, 1, 2, 3, 4});
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testEncodeAndDecode_FilledBodyWithExtFields() throws RemotingCommandException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new ExtFieldsHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        cmd.addExtField("key", "value");

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            assertThat(decodedCommand.getExtFields().get("stringValue")).isEqualTo("bilibili");
            assertThat(decodedCommand.getExtFields().get("intValue")).isEqualTo("2333");
            assertThat(decodedCommand.getExtFields().get("longValue")).isEqualTo("23333333");
            assertThat(decodedCommand.getExtFields().get("booleanValue")).isEqualTo("true");
            assertThat(decodedCommand.getExtFields().get("doubleValue")).isEqualTo("0.618");

            assertThat(decodedCommand.getExtFields().get("key")).isEqualTo("value");

            CommandCustomHeader decodedHeader = decodedCommand.decodeCommandCustomHeader(ExtFieldsHeader.class);
            assertThat(((ExtFieldsHeader) decodedHeader).getStringValue()).isEqualTo("bilibili");
            assertThat(((ExtFieldsHeader) decodedHeader).getIntValue()).isEqualTo(2333);
            assertThat(((ExtFieldsHeader) decodedHeader).getLongValue()).isEqualTo(23333333L);
            assertThat(((ExtFieldsHeader) decodedHeader).isBooleanValue()).isEqualTo(true);
            assertThat(((ExtFieldsHeader) decodedHeader).getDoubleValue()).isBetween(0.617, 0.619);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assert.fail("Should not throw IOException");
        }

    }

    @Test
    public void testNotNullField() throws Exception {
        RemotingCommand remotingCommand = new RemotingCommand();
        Method method = RemotingCommand.class.getDeclaredMethod("isFieldNullable", Field.class);
        method.setAccessible(true);

        Field nullString = FieldTestClass.class.getDeclaredField("nullString");
        assertThat(method.invoke(remotingCommand, nullString)).isEqualTo(false);

        Field nullableString = FieldTestClass.class.getDeclaredField("nullable");
        assertThat(method.invoke(remotingCommand, nullableString)).isEqualTo(true);

        Field value = FieldTestClass.class.getDeclaredField("value");
        assertThat(method.invoke(remotingCommand, value)).isEqualTo(false);
    }

    @Test
    public void testParentField() throws Exception {
        SubExtFieldsHeader subExtFieldsHeader = new SubExtFieldsHeader();
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(1, subExtFieldsHeader);
        Field[] fields  = remotingCommand.getClazzFields(subExtFieldsHeader.getClass());
        Set<String> fieldNames = new HashSet<>();
        for (Field field: fields) {
            fieldNames.add(field.getName());
        }
        Assert.assertTrue(fields.length >= 7);
        Set<String> names = new HashSet<>();
        names.add("stringValue");
        names.add("intValue");
        names.add("longValue");
        names.add("booleanValue");
        names.add("doubleValue");
        names.add("name");
        names.add("value");
        for (String name: names) {
            Assert.assertTrue(fieldNames.contains(name));
        }
        remotingCommand.makeCustomHeaderToNet();
        SubExtFieldsHeader other = (SubExtFieldsHeader) remotingCommand.decodeCommandCustomHeader(subExtFieldsHeader.getClass());
        Assert.assertEquals(other, subExtFieldsHeader);
    }
}

class FieldTestClass {
    @CFNotNull
    String nullString = null;

    String nullable = null;

    @CFNotNull
    String value = "NotNull";
}

class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

class ExtFieldsHeader implements CommandCustomHeader {
    private String stringValue = "bilibili";
    private int intValue = 2333;
    private long longValue = 23333333L;
    private boolean booleanValue = true;
    private double doubleValue = 0.618;

    @Override
    public void checkFields() throws RemotingCommandException {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExtFieldsHeader)) return false;

        ExtFieldsHeader that = (ExtFieldsHeader) o;

        if (intValue != that.intValue) return false;
        if (longValue != that.longValue) return false;
        if (booleanValue != that.booleanValue) return false;
        if (Double.compare(that.doubleValue, doubleValue) != 0) return false;
        return stringValue != null ? stringValue.equals(that.stringValue) : that.stringValue == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = stringValue != null ? stringValue.hashCode() : 0;
        result = 31 * result + intValue;
        result = 31 * result + (int) (longValue ^ (longValue >>> 32));
        result = 31 * result + (booleanValue ? 1 : 0);
        temp = Double.doubleToLongBits(doubleValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}


class SubExtFieldsHeader extends ExtFieldsHeader {
    private String name = "12321";
    private int value = 111;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SubExtFieldsHeader)) return false;
        if (!super.equals(o)) return false;

        SubExtFieldsHeader that = (SubExtFieldsHeader) o;

        if (value != that.value) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + value;
        return result;
    }
}
