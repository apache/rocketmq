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

package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class FileRegionEncoderTest {

    /**
     * This unit test case ensures that {@link FileRegionEncoder} indeed wraps {@link FileRegion} to
     * {@link ByteBuf}.
     * @throws IOException if there is an error.
     */
    @Test
    public void testEncode() throws IOException {
        FileRegionEncoder fileRegionEncoder = new FileRegionEncoder();
        EmbeddedChannel channel = new EmbeddedChannel(fileRegionEncoder);
        File file = File.createTempFile(UUID.randomUUID().toString(), ".data");
        file.deleteOnExit();
        Random random = new Random(System.currentTimeMillis());
        int dataLength = 1 << 10;
        byte[] data = new byte[dataLength];
        random.nextBytes(data);
        write(file, data);
        FileRegion fileRegion = new DefaultFileRegion(file, 0, dataLength);
        Assert.assertEquals(0, fileRegion.transferred());
        Assert.assertEquals(dataLength, fileRegion.count());
        Assert.assertTrue(channel.writeOutbound(fileRegion));
        ByteBuf out = (ByteBuf) channel.readOutbound();
        byte[] arr = new byte[out.readableBytes()];
        out.getBytes(0, arr);
        Assert.assertArrayEquals("Data should be identical", data, arr);
    }

    /**
     * Write byte array to the specified file.
     *
     * @param file File to write to.
     * @param data byte array to write.
     * @throws IOException in case there is an exception.
     */
    private static void write(File file, byte[] data) throws IOException {
        BufferedOutputStream bufferedOutputStream = null;
        try {
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file, false));
            bufferedOutputStream.write(data);
            bufferedOutputStream.flush();
        } finally {
            if (null != bufferedOutputStream) {
                bufferedOutputStream.close();
            }
        }
    }
}
