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

package org.apache.rocketmq.tools.command.message;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.Test;

public class DecodeMessageIdCommandTest {
    @Test
    public void testExecuteDecodeUniqueMessageIdByDefault() throws Exception {
        String uniqueMessageId = MessageClientIDSetter.createUniqID();
        Thread.sleep(5L);
        String output = execute("-i", uniqueMessageId);

        Assert.assertEquals(String.format("IP: %s%nDate: %s%n",
            MessageClientIDSetter.getIPStrFromID(uniqueMessageId),
            UtilAll.formatDate(MessageClientIDSetter.getNearlyTimeFromID(uniqueMessageId), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS)),
            output);
    }

    @Test
    public void testExecuteDecodeDocumentedOffsetMessageId() throws SubCommandException {
        String output = execute("-i", "0A42333A00002A9F000000000134F1F5", "-t", "offset");

        Assert.assertEquals(String.format("StoreHost: 10.66.51.58:10911%nCommitLogOffset: 20247029%n"), output);
    }

    @Test
    public void testExecuteDecodeIPv4OffsetMessageId() throws Exception {
        long commitLogOffset = 123456L;
        String offsetMessageId = MessageDecoder.createMessageId(new InetSocketAddress("127.0.0.1", 10911), commitLogOffset);
        MessageId decodedMessageId = MessageDecoder.decodeMessageId(offsetMessageId);

        String output = execute("-i", offsetMessageId, "-t", "offset");

        Assert.assertEquals(String.format("StoreHost: %s%nCommitLogOffset: %d%n",
            formatStoreHost(decodedMessageId), commitLogOffset), output);
    }

    @Test
    public void testExecuteDecodeIPv6OffsetMessageId() throws Exception {
        long commitLogOffset = 654321L;
        String offsetMessageId = MessageDecoder.createMessageId(
            new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 10912), commitLogOffset);
        Assert.assertEquals(56, offsetMessageId.length());

        String output = execute("-i", offsetMessageId, "-t", "offset");

        Assert.assertEquals(String.format("StoreHost: [1050:0:0:0:5:600:300c:326b]:10912%nCommitLogOffset: 654321%n"), output);
    }

    @Test
    public void testExecuteWithUnsupportedMessageIdType() throws SubCommandException {
        try {
            execute("-i", MessageClientIDSetter.createUniqID(), "-t", "unknown");
            Assert.fail("Should fail when messageIdType is unsupported");
        } catch (SubCommandException e) {
            Assert.assertTrue(e.getMessage().contains("Unsupported messageIdType: unknown"));
        }
    }

    private String execute(String... subargs) throws SubCommandException {
        DecodeMessageIdCommand cmd = new DecodeMessageIdCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        try {
            cmd.execute(commandLine, options, null);
            return new String(bos.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            System.setOut(out);
        }
    }

    private String formatStoreHost(MessageId messageId) {
        InetSocketAddress address = (InetSocketAddress) messageId.getAddress();
        return NetworkUtil.normalizeHostAddress(address.getAddress()) + ":" + address.getPort();
    }
}
