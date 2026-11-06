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

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DumpCompactionLogCommandTest {

    private DumpCompactionLogCommand command;
    private File tempFile;

    @Before
    public void setUp() throws Exception {
        command = new DumpCompactionLogCommand();
        tempFile = File.createTempFile("compaction-log-test", ".log");
    }

    @After
    public void tearDown() {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testExecuteWithValidFile() throws Exception {
        // Write a valid message to the temp file
        MessageExt msg = new MessageExt();
        msg.setBody("test-body".getBytes());
        msg.setTopic("test-topic");
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 9000));
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 9000));
        byte[] encoded = MessageDecoder.encode(msg, false);

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.wrap(encoded);
            channel.write(buffer);
        }

        Options options = command.buildCommandlineOptions(new Options());
        String[] args = {"-f", tempFile.getAbsolutePath()};
        CommandLine commandLine = new DefaultParser().parse(options, args);

        // Should not throw any exception and should not leak file descriptors
        command.execute(commandLine, options, null);

        // Verify the temp file can still be accessed (not locked by leaked handles)
        Assert.assertTrue(tempFile.exists());
        Assert.assertTrue(tempFile.canRead());
    }

    @Test
    public void testExecuteWithEmptyFile() throws Exception {
        Options options = command.buildCommandlineOptions(new Options());
        String[] args = {"-f", tempFile.getAbsolutePath()};
        CommandLine commandLine = new DefaultParser().parse(options, args);

        // Should handle empty file gracefully without resource leak
        command.execute(commandLine, options, null);
        Assert.assertTrue(tempFile.exists());
    }

    @Test(expected = SubCommandException.class)
    public void testExecuteWithNonExistentFile() throws Exception {
        Options options = command.buildCommandlineOptions(new Options());
        String[] args = {"-f", "/non/existent/file.log"};
        CommandLine commandLine = new DefaultParser().parse(options, args);

        command.execute(commandLine, options, null);
    }

    @Test
    public void testExecuteWithoutFileOption() throws Exception {
        Options options = command.buildCommandlineOptions(new Options());
        String[] args = {};
        CommandLine commandLine = new DefaultParser().parse(options, args);

        // Should print error message, not throw
        command.execute(commandLine, options, null);
    }
}
