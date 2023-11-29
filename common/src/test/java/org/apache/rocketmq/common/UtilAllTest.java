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

package org.apache.rocketmq.common;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtilAllTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testCurrentStackTrace() {
        String currentStackTrace = UtilAll.currentStackTrace();
        assertThat(currentStackTrace).contains("UtilAll.currentStackTrace");
        assertThat(currentStackTrace).contains("UtilAllTest.testCurrentStackTrace(");
    }

    @Test
    public void testProperties2Object() {
        DemoConfig demoConfig = new DemoConfig();
        Properties properties = new Properties();
        properties.setProperty("demoWidth", "123");
        properties.setProperty("demoLength", "456");
        properties.setProperty("demoOK", "true");
        properties.setProperty("demoName", "TestDemo");
        MixAll.properties2Object(properties, demoConfig);
        assertThat(demoConfig.getDemoLength()).isEqualTo(456);
        assertThat(demoConfig.getDemoWidth()).isEqualTo(123);
        assertThat(demoConfig.isDemoOK()).isTrue();
        assertThat(demoConfig.getDemoName()).isEqualTo("TestDemo");
    }

    @Test
    public void testProperties2String() {
        DemoSubConfig demoConfig = new DemoSubConfig();
        demoConfig.setDemoLength(123);
        demoConfig.setDemoWidth(456);
        demoConfig.setDemoName("TestDemo");
        demoConfig.setDemoOK(true);

        demoConfig.setSubField0("1");
        demoConfig.setSubField1(false);

        Properties properties = MixAll.object2Properties(demoConfig);
        assertThat(properties.getProperty("demoLength")).isEqualTo("123");
        assertThat(properties.getProperty("demoWidth")).isEqualTo("456");
        assertThat(properties.getProperty("demoOK")).isEqualTo("true");
        assertThat(properties.getProperty("demoName")).isEqualTo("TestDemo");

        assertThat(properties.getProperty("subField0")).isEqualTo("1");
        assertThat(properties.getProperty("subField1")).isEqualTo("false");

        properties = MixAll.object2Properties(new Object());
        assertEquals(0, properties.size());
    }

    @Test
    public void testIsPropertiesEqual() {
        final Properties p1 = new Properties();
        final Properties p2 = new Properties();

        p1.setProperty("a", "1");
        p1.setProperty("b", "2");
        p2.setProperty("a", "1");
        p2.setProperty("b", "2");

        assertThat(MixAll.isPropertiesEqual(p1, p2)).isTrue();
    }

    @Test
    public void testGetPid() {
        assertThat(UtilAll.getPid()).isGreaterThan(0);
    }

    @Test
    public void testGetDiskPartitionSpaceUsedPercent() {
        String tmpDir = System.getProperty("java.io.tmpdir");

        assertThat(UtilAll.getDiskPartitionSpaceUsedPercent(null)).isCloseTo(-1, within(0.000001));
        assertThat(UtilAll.getDiskPartitionSpaceUsedPercent("")).isCloseTo(-1, within(0.000001));
        assertThat(UtilAll.getDiskPartitionSpaceUsedPercent("nonExistingPath")).isCloseTo(-1, within(0.000001));
        assertThat(UtilAll.getDiskPartitionSpaceUsedPercent(tmpDir)).isNotCloseTo(-1, within(0.000001));
    }

    @Test
    public void testIsBlank() {
        assertThat(UtilAll.isBlank("Hello ")).isFalse();
        assertThat(UtilAll.isBlank(" Hello")).isFalse();
        assertThat(UtilAll.isBlank("He llo")).isFalse();
        assertThat(UtilAll.isBlank("  ")).isTrue();
        assertThat(UtilAll.isBlank("Hello")).isFalse();
    }

    @Test
    public void testIPv6Check() throws UnknownHostException {
        InetAddress nonInternal = InetAddress.getByName("2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
        InetAddress internal = InetAddress.getByName("FE80:0000:0000:0000:0000:0000:0000:FFFF");
        assertThat(UtilAll.isInternalV6IP(nonInternal)).isFalse();
        assertThat(UtilAll.isInternalV6IP(internal)).isTrue();
        assertThat(UtilAll.ipToIPv6Str(nonInternal.getAddress()).toUpperCase()).isEqualTo("2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
    }

    @Test
    public void testJoin() {
        List<String> list = Arrays.asList("groupA=DENY", "groupB=PUB|SUB", "groupC=SUB");
        String comma = ",";
        assertEquals("groupA=DENY,groupB=PUB|SUB,groupC=SUB", UtilAll.join(list, comma));
        assertEquals(null, UtilAll.join(null, comma));
        List<String> objects = Collections.emptyList();
        assertEquals("", UtilAll.join(objects, comma));
    }

    static class DemoConfig {
        private int demoWidth = 0;
        private int demoLength = 0;
        private boolean demoOK = false;
        private String demoName = "haha";

        int getDemoWidth() {
            return demoWidth;
        }

        public void setDemoWidth(int demoWidth) {
            this.demoWidth = demoWidth;
        }

        public int getDemoLength() {
            return demoLength;
        }

        public void setDemoLength(int demoLength) {
            this.demoLength = demoLength;
        }

        public boolean isDemoOK() {
            return demoOK;
        }

        public void setDemoOK(boolean demoOK) {
            this.demoOK = demoOK;
        }

        public String getDemoName() {
            return demoName;
        }

        public void setDemoName(String demoName) {
            this.demoName = demoName;
        }

        @Override
        public String toString() {
            return "DemoConfig{" +
                "demoWidth=" + demoWidth +
                ", demoLength=" + demoLength +
                ", demoOK=" + demoOK +
                ", demoName='" + demoName + '\'' +
                '}';
        }
    }

    static class DemoSubConfig extends DemoConfig {
        private String subField0 = "0";
        public boolean subField1 = true;

        public String getSubField0() {
            return subField0;
        }

        public void setSubField0(String subField0) {
            this.subField0 = subField0;
        }

        public boolean isSubField1() {
            return subField1;
        }

        public void setSubField1(boolean subField1) {
            this.subField1 = subField1;
        }
    }

    @Test
    public void testCleanBuffer() {
        UtilAll.cleanBuffer(null);
        UtilAll.cleanBuffer(ByteBuffer.allocateDirect(10));
        UtilAll.cleanBuffer(ByteBuffer.allocateDirect(0));
        UtilAll.cleanBuffer(ByteBuffer.allocate(10));
    }

    @Test
    public void testCalculateFileSizeInPath() throws Exception {
        /**
         * testCalculateFileSizeInPath
         *  - file_0
         *  - dir_1
         *      - file_1_0
         *      - file_1_1
         *      - dir_1_2
         *          - file_1_2_0
         *  - dir_2
         */
        File baseFile = tempDir.getRoot();

        // test empty path
        assertEquals(0, UtilAll.calculateFileSizeInPath(baseFile));

        File file0 = new File(baseFile, "file_0");
        assertTrue(file0.createNewFile());
        writeFixedBytesToFile(file0, 1313);

        assertEquals(1313, UtilAll.calculateFileSizeInPath(baseFile));

        // build a file tree like above
        File dir1 = new File(baseFile, "dir_1");
        dir1.mkdirs();
        File file10 = new File(dir1, "file_1_0");
        File file11 = new File(dir1, "file_1_1");
        File dir12 = new File(dir1, "dir_1_2");
        dir12.mkdirs();
        File file120 = new File(dir12, "file_1_2_0");
        File dir2 = new File(baseFile, "dir_2");
        dir2.mkdirs();

        // write all file with 1313 bytes data
        assertTrue(file10.createNewFile());
        writeFixedBytesToFile(file10, 1313);
        assertTrue(file11.createNewFile());
        writeFixedBytesToFile(file11, 1313);
        assertTrue(file120.createNewFile());
        writeFixedBytesToFile(file120, 1313);

        assertEquals(1313 * 4, UtilAll.calculateFileSizeInPath(baseFile));
    }

    private void writeFixedBytesToFile(File file, int size) throws Exception {
        FileOutputStream outputStream = new FileOutputStream(file);
        byte[] bytes = new byte[size];
        outputStream.write(bytes, 0, size);
        outputStream.close();
    }
}
