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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RocketMQSerializableTest {
    @Test
    public void testRocketMQProtocolEncodeAndDecode_WithoutRemarkWithoutExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        assertThat(result).hasSize(21);
        assertThat(parseToShort(result, 0)).isEqualTo((short) code); //code
        assertThat(result[2]).isEqualTo(LanguageCode.JAVA.getCode()); //language
        assertThat(parseToShort(result, 3)).isEqualTo((short) 2333); //version
        assertThat(parseToInt(result, 9)).isEqualTo(0); //flag
        assertThat(parseToInt(result, 13)).isEqualTo(0); //empty remark
        assertThat(parseToInt(result, 17)).isEqualTo(0); //empty extFields

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(Unpooled.wrappedBuffer(result), result.length);

            assertThat(decodedCommand.getCode()).isEqualTo(code);
            assertThat(decodedCommand.getLanguage()).isEqualTo(LanguageCode.JAVA);
            assertThat(decodedCommand.getVersion()).isEqualTo(2333);
            assertThat(decodedCommand.getOpaque()).isEqualTo(opaque);
            assertThat(decodedCommand.getFlag()).isEqualTo(0);
            assertThat(decodedCommand.getRemark()).isNull();
            assertThat(decodedCommand.getExtFields()).isNull();
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testRocketMQProtocolEncodeAndDecode_WithRemarkWithoutExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code,
            new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);
        cmd.setRemark("Sample Remark");

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        assertThat(result).hasSize(34);
        assertThat(parseToShort(result, 0)).isEqualTo((short) code); //code
        assertThat(result[2]).isEqualTo(LanguageCode.JAVA.getCode()); //language
        assertThat(parseToShort(result, 3)).isEqualTo((short) 2333); //version
        assertThat(parseToInt(result, 9)).isEqualTo(0); //flag
        assertThat(parseToInt(result, 13)).isEqualTo(13); //remark length

        byte[] remarkArray = new byte[13];
        System.arraycopy(result, 17, remarkArray, 0, 13);
        assertThat(new String(remarkArray)).isEqualTo("Sample Remark");

        assertThat(parseToInt(result, 30)).isEqualTo(0); //empty extFields

        try {
            RemotingCommand decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(Unpooled.wrappedBuffer(result), result.length);

            assertThat(decodedCommand.getCode()).isEqualTo(code);
            assertThat(decodedCommand.getLanguage()).isEqualTo(LanguageCode.JAVA);
            assertThat(decodedCommand.getVersion()).isEqualTo(2333);
            assertThat(decodedCommand.getOpaque()).isEqualTo(opaque);
            assertThat(decodedCommand.getFlag()).isEqualTo(0);
            assertThat(decodedCommand.getRemark()).contains("Sample Remark");
            assertThat(decodedCommand.getExtFields()).isNull();
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testRocketMQProtocolEncodeAndDecode_WithoutRemarkWithExtFields() throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.remoting.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code,
            new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);
        cmd.addExtField("key", "value");

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        assertThat(result).hasSize(35);
        assertThat(parseToShort(result, 0)).isEqualTo((short) code); //code
        assertThat(result[2]).isEqualTo(LanguageCode.JAVA.getCode()); //language
        assertThat(parseToShort(result, 3)).isEqualTo((short) 2333); //version
        assertThat(parseToInt(result, 9)).isEqualTo(0); //flag
        assertThat(parseToInt(result, 13)).isEqualTo(0); //empty remark
        assertThat(parseToInt(result, 17)).isEqualTo(14); //extFields length

        byte[] extFieldsArray = new byte[14];
        System.arraycopy(result, 21, extFieldsArray, 0, 14);
        HashMap<String, String> extFields =
                RocketMQSerializable.mapDeserialize(Unpooled.wrappedBuffer(extFieldsArray), extFieldsArray.length);
        assertThat(extFields).contains(new HashMap.SimpleEntry("key", "value"));

        try {
            RemotingCommand decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(Unpooled.wrappedBuffer(result), result.length);
            assertThat(decodedCommand.getCode()).isEqualTo(code);
            assertThat(decodedCommand.getLanguage()).isEqualTo(LanguageCode.JAVA);
            assertThat(decodedCommand.getVersion()).isEqualTo(2333);
            assertThat(decodedCommand.getOpaque()).isEqualTo(opaque);
            assertThat(decodedCommand.getFlag()).isEqualTo(0);
            assertThat(decodedCommand.getRemark()).isNull();
            assertThat(decodedCommand.getExtFields()).contains(new HashMap.SimpleEntry("key", "value"));
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assert.fail("Should not throw IOException");
        }
    }

    private short parseToShort(byte[] array, int index) {
        return (short) (array[index] * 256 + array[++index]);
    }

    private int parseToInt(byte[] array, int index) {
        return array[index] * 16777216 + array[++index] * 65536 + array[++index] * 256
            + array[++index];
    }

    public static class MyHeader1 implements CommandCustomHeader {
        private String str;
        private int num;

        @Override
        public void checkFields() throws RemotingCommandException {
        }

        public String getStr() {
            return str;
        }

        public void setStr(String str) {
            this.str = str;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }
    }

    @Test
    public void testFastEncode() throws Exception {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(16);
        MyHeader1 header1 = new MyHeader1();
        header1.setStr("s1");
        header1.setNum(100);
        RemotingCommand cmd = RemotingCommand.createRequestCommand(1, header1);
        cmd.setRemark("remark");
        cmd.setOpaque(1001);
        cmd.setVersion(99);
        cmd.setLanguage(LanguageCode.JAVA);
        cmd.setFlag(3);
        cmd.makeCustomHeaderToNet();
        RocketMQSerializable.rocketMQProtocolEncode(cmd, buf);
        RemotingCommand cmd2 = RocketMQSerializable.rocketMQProtocolDecode(buf, buf.readableBytes());
        assertThat(cmd2.getRemark()).isEqualTo("remark");
        assertThat(cmd2.getCode()).isEqualTo(1);
        assertThat(cmd2.getOpaque()).isEqualTo(1001);
        assertThat(cmd2.getVersion()).isEqualTo(99);
        assertThat(cmd2.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(cmd2.getFlag()).isEqualTo(3);

        MyHeader1 h2 = (MyHeader1) cmd2.decodeCommandCustomHeader(MyHeader1.class);
        assertThat(h2.getStr()).isEqualTo("s1");
        assertThat(h2.getNum()).isEqualTo(100);
    }
}
