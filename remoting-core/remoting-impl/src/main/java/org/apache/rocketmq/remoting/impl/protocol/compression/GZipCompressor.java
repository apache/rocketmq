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

package org.apache.rocketmq.remoting.impl.protocol.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.rocketmq.remoting.api.compressable.Compressor;

public class GZipCompressor implements Compressor {
    public static final int BUFFER = 1024;
    public static final String COMPRESSOR_NAME = GZipCompressor.class.getSimpleName();
    public static final byte COMPRESSOR_TYPE = 'G';

    @Override
    public String name() {
        return COMPRESSOR_NAME;
    }

    @Override
    public byte type() {
        return COMPRESSOR_TYPE;
    }

    @Override
    public byte[] compress(byte[] content) throws Exception {
        if (content == null)
            return new byte[0];

        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        compress(bais, baos);
        byte[] output = baos.toByteArray();
        baos.flush();
        baos.close();
        bais.close();
        return output;

    }

    private void compress(InputStream is, OutputStream os) throws Exception {
        GZIPOutputStream gos = new GZIPOutputStream(os);

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = is.read(data, 0, BUFFER)) != -1) {
            gos.write(data, 0, count);
        }
        gos.finish();
        gos.flush();
        gos.close();
    }

    @Override
    public byte[] deCompress(byte[] content) throws Exception {
        if (content == null)
            return new byte[0];

        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        decompress(bais, baos);
        content = baos.toByteArray();
        baos.flush();
        baos.close();
        bais.close();
        return content;
    }

    private void decompress(InputStream is, OutputStream os) throws Exception {
        GZIPInputStream gis = new GZIPInputStream(is);

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = gis.read(data, 0, BUFFER)) != -1) {
            os.write(data, 0, count);
        }
        gis.close();
    }

}
