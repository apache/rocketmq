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

package org.apache.rocketmq.thinclient.misc;

import apache.rocketmq.v2.ReceiveMessageRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.commons.lang3.StringUtils;

public class Utilities {
    public static final int MASTER_BROKER_ID = 0;

    public static final Locale LOCALE = new Locale("zh", "CN");

    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";

    private static final Random RANDOM = new SecureRandom();
    private static final int PROCESS_ID_NOT_SET = -2;
    private static final int PROCESS_ID_NOT_FOUND = -1;
    private static int processId = PROCESS_ID_NOT_SET;

    private static final String HOST_NAME_NOT_FOUND = "HOST_NAME_NOT_FOUND";

    private static String protocolVersion = null;
    private static String hostName = null;
    private static byte[] macAddress = null;

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_LOWER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * Used to build output as Hex
     */
    private static final char[] DIGITS_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private static final String CLIENT_ID_SEPARATOR = "@";

    private Utilities() {
    }

    public static byte[] macAddress() {
        if (null != macAddress) {
            return macAddress.clone();
        }
        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                final byte[] mac = networkInterface.getHardwareAddress();
                if (null == mac) {
                    continue;
                }
                macAddress = mac;
                return macAddress.clone();
            }
        } catch (Throwable ignore) {
            // Ignore on purpose.
        }
        byte[] randomBytes = new byte[6];
        RANDOM.nextBytes(randomBytes);
        macAddress = randomBytes;
        return macAddress.clone();
    }

    public static String getProtocolVersion() {
        if (null != protocolVersion) {
            return protocolVersion;
        }
        protocolVersion = ReceiveMessageRequest.class.getName().split("\\.")[2];
        return protocolVersion;
    }

    public static int processId() {
        if (processId != PROCESS_ID_NOT_SET) {
            return processId;
        }
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        // Format: "pid@hostname"
        String name = runtime.getName();
        try {
            processId = Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Throwable ignore) {
            processId = PROCESS_ID_NOT_FOUND;
        }
        return processId;
    }

    public static String hostName() {
        if (null != hostName) {
            return hostName;
        }
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            return hostName;
        } catch (Throwable ignore) {
            hostName = HOST_NAME_NOT_FOUND;
            return hostName;
        }
    }

    public static byte[] compressBytesGzip(final byte[] src, final int level) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater defeater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream =
            new DeflaterOutputStream(byteArrayOutputStream, defeater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            defeater.end();
        }
    }

    public static byte[] uncompressBytesGzip(final byte[] src) throws IOException {
        byte[] uncompressData = new byte[src.length];

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            int length;
            while ((length = inflaterInputStream.read(uncompressData, 0, uncompressData.length)) > 0) {
                byteArrayOutputStream.write(uncompressData, 0, length);
            }
            byteArrayOutputStream.flush();

            return byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            try {
                inflaterInputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
                // Exception not expected here.
            }
        }
    }

    public static String encodeHexString(ByteBuffer byteBuffer, boolean toLowerCase) {
        return new String(encodeHex(byteBuffer, toLowerCase));
    }

    public static char[] encodeHex(ByteBuffer byteBuffer, boolean toLowerCase) {
        return encodeHex(byteBuffer, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    public static String encodeHexString(final byte[] data, final boolean toLowerCase) {
        return new String(encodeHex(data, toLowerCase));
    }

    public static char[] encodeHex(final byte[] data, final boolean toLowerCase) {
        return encodeHex(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
    }

    protected static char[] encodeHex(final ByteBuffer data, final char[] toDigits) {
        return encodeHex(data.array(), toDigits);
    }

    protected static char[] encodeHex(final byte[] data, final char[] toDigits) {
        final int l = data.length;
        final char[] out = new char[l << 1];
        // Two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
            out[j++] = toDigits[0x0F & data[i]];
        }
        return out;
    }

    public static String crc32CheckSum(byte[] array) {
        CRC32 crc32 = new CRC32();
        // Do not use crc32.update(array) directly for the compatibility, which has been marked as 'since Java1.9'.
        crc32.update(array, 0, array.length);
        return Long.toHexString(crc32.getValue()).toUpperCase(LOCALE);
    }

    public static String md5CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(array);
        return encodeHexString(digest.digest(), false);
    }

    public static String sha1CheckSum(byte[] array) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(array);
        return encodeHexString(digest.digest(), false);
    }

    public static String stackTrace() {
        return stackTrace(Thread.getAllStackTraces());
    }

    public static String stackTrace(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(),
                        thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s%s%n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable e) {
            result.append(e);
        }
        return result.toString();
    }

    public static String genClientId() {
        StringBuilder sb = new StringBuilder();
        final String hostName = Utilities.hostName();
        sb.append(hostName);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Utilities.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Long.toString(System.nanoTime(), 36));
        return sb.toString();
    }

    public static String getOsDescription() {
        final String osName = Utilities.getOsName();
        if (null == osName) {
            return StringUtils.EMPTY;
        }
        String version = Utilities.getOsVersion();
        return null != version ? osName + StringUtils.SPACE + version : osName;
    }

    public static String getOsName() {
        try {
            return System.getProperty(OS_NAME);
        } catch (Throwable t) {
            // ignore on purpose.
            return null;
        }
    }

    public static String getOsVersion() {
        try {
            return System.getProperty(OS_VERSION);
        } catch (Throwable t) {
            // ignore on purpose.
            return null;
        }
    }
}
