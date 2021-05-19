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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class UtilAllTests {

    @Rule
    private TemporaryFolder folder = new TemporaryFolder();

    @Rule
    private ExpectedException thrown = ExpectedException.none();

    @PrepareForTest({RuntimeMXBean.class,
            UtilAll.class, ManagementFactory.class})
    @Test
    public void testGetPidIfNotNull() {
        PowerMockito.mockStatic(ManagementFactory.class);
        RuntimeMXBean runtimeMXBean = PowerMockito.mock(RuntimeMXBean.class);
        PowerMockito.when(runtimeMXBean.getName())
                .thenReturn("40@abc.com");
        PowerMockito.doReturn(runtimeMXBean).when(ManagementFactory.class);
        ManagementFactory.getRuntimeMXBean();

        Assert.assertEquals(40, UtilAll.getPid());
    }

    @PrepareForTest({RuntimeMXBean.class,
            UtilAll.class, ManagementFactory.class})
    @Test
    public void testGetPidIfNull() {
        PowerMockito.mockStatic(ManagementFactory.class);
        RuntimeMXBean runtimeMXBean = PowerMockito.mock(RuntimeMXBean.class);
        PowerMockito.when(runtimeMXBean.getName()).thenReturn(null);
        PowerMockito.doReturn(runtimeMXBean).when(ManagementFactory.class);
        ManagementFactory.getRuntimeMXBean();

        Assert.assertEquals(-1, UtilAll.getPid());
    }

    @PrepareForTest({UtilAll.class})
    @Test
    public void testCurrentStackTraceIfNotNull() {
        PowerMockito.mockStatic(Thread.class);
        StackTraceElement stackTraceElement =
                new StackTraceElement("Bar", "foo", "baz", 101);
        PowerMockito.when(Thread.currentThread().getStackTrace())
                .thenReturn(new StackTraceElement[]{stackTraceElement});

        Assert.assertEquals("\n\tBar.foo(baz:101)",
                UtilAll.currentStackTrace());
    }

    @Test
    public void testOffset2FileNameIfInputNotNull() {
        Assert.assertEquals("00000000000000000002",
                UtilAll.offset2FileName(2l));
        Assert.assertEquals("00000001558787014000",
                UtilAll.offset2FileName(1558787014000l));
    }

    @PrepareForTest({UtilAll.class, System.class})
    @Test
    public void testComputeEclipseTimeMillisecondsIfInputNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1558787014000l);

        Assert.assertEquals(2592000000l,
                UtilAll.computeEclipseTimeMilliseconds(1556195014000l));
    }

    @PrepareForTest({Calendar.class, UtilAll.class})
    @Test
    public void testIsItTimeToDoIfCurrentSetTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(1558787014000l);
        PowerMockito.mockStatic(Calendar.class);
        PowerMockito.when(Calendar.getInstance()).thenReturn(calendar);

        Assert.assertTrue(UtilAll.isItTimeToDo("13;12;14"));

        Assert.assertFalse(UtilAll.isItTimeToDo("00;00;00;"));
    }

    @PrepareForTest({UtilAll.class, System.class})
    @Test
    public void testTimeMillisToHumanStringIfCurrentTimeNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1558787014000l);

        Assert.assertEquals("20190525132334000",
                UtilAll.timeMillisToHumanString());
    }

    @Test
    public void testTimeMillisToHumanStringLongIfInputNotNull() {
        Assert.assertEquals("20190525132334000",
                UtilAll.timeMillisToHumanString(1558787014000l));
    }

    @PrepareForTest({System.class, UtilAll.class})
    @Test
    public void testComputNextMorningTimeMillisIfCurrentTimeNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1515585600000L);

        Assert.assertEquals(1515628800000l,
                UtilAll.computNextMorningTimeMillis());
    }

    @PrepareForTest({System.class, UtilAll.class})
    @Test
    public void testComputNextMinutesTimeMillisIfCurrentTimeNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1515585600000L);

        Assert.assertEquals(1515585660000l,
                UtilAll.computNextMinutesTimeMillis());
    }

    @PrepareForTest({System.class, UtilAll.class})
    @Test
    public void testComputNextHourTimeMillisIfCurrentTimeNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1515585600000L);

        Assert.assertEquals(1515589200000l,
                UtilAll.computNextHourTimeMillis());
    }

    @PrepareForTest({UtilAll.class, System.class})
    @Test
    public void testComputNextHalfHourTimeMillisIfCurrentTimeNotNull() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis())
                .thenReturn(1515585600000L);

        Assert.assertEquals(1515591000000l,
                UtilAll.computNextHalfHourTimeMillis());
    }

    @Test
    public void testTimeMillisToHumanString2IfInputNotNull() {
        Assert.assertEquals("2019-04-25 13:23:34,000",
                UtilAll.timeMillisToHumanString2(1556195014000l));
    }

    @Test
    public void testTimeMillisToHumanString3IfInputNotNull() {
        Assert.assertEquals("20190425132334",
                UtilAll.timeMillisToHumanString3(1556195014000l));
    }

    @Test
    public void testGetDiskPartitionSpaceUsedPercentIfFileDoesNotExist() {
        Assert.assertEquals(-1,
                UtilAll.getDiskPartitionSpaceUsedPercent(""), 0);
        Assert.assertEquals(-1,
                UtilAll.getDiskPartitionSpaceUsedPercent(null), 0);
        Assert.assertEquals(-1,
                UtilAll.getDiskPartitionSpaceUsedPercent("foo"), 0);
    }

    @Test
    public void testGetDiskPartitionSpaceUsedPercentIfFileExists()
            throws IOException {
        File file = folder.newFile("foo.txt");
        try {
            Assert.assertNotNull(
                    UtilAll.getDiskPartitionSpaceUsedPercent(file.getPath()));
        } finally {
            file.delete();
            folder.delete();
        }
    }

    @Test
    public void testCrc32IfInputNull() {
        Assert.assertEquals(0, UtilAll.crc32(null));
        Assert.assertEquals(0, UtilAll.crc32(new byte[]{}));
    }

    @Test
    public void testCrc32IfInputNotNull() {
        Assert.assertEquals(1438416925, UtilAll.crc32(new byte[]{1, 2, 3}));
    }

    @Test
    public void testBytes2stringIfInputArrayNotEmpty() {
        Assert.assertEquals("010203",
                UtilAll.bytes2string(new byte[]{1, 2, 3}));
    }

    @Test
    public void testBytes2stringIfInputEmptyArray() {
        Assert.assertEquals("", UtilAll.bytes2string(new byte[0]));
    }

    @Test
    public void testString2bytesIfInputNullOrEmptyString() {
        Assert.assertNull(UtilAll.string2bytes(""));
        Assert.assertNull(UtilAll.string2bytes(null));
    }

    @Test
    public void testString2bytesIfInputNotNullOrEmptyString() {
        Assert.assertArrayEquals(new byte[]{-1}, UtilAll.string2bytes("foo"));
    }

    @Test
    public void testUncompressIfInputNotNull() throws IOException {
        byte[] by = new byte[]{120, 1, 1, 3, 0, -4, -1, 1, 2, 3, 0, 13, 0, 7};
        Assert.assertArrayEquals(new byte[]{1, 2, 3}, UtilAll.uncompress(by));
    }

    @Test
    public void testCompressIfInputNotNull() throws IOException {
        byte[] by = new byte[]{120, 1, 1, 3, 0, -4, -1, 1, 2, 3, 0, 13, 0, 7};
        Assert.assertArrayEquals(by, UtilAll.compress(new byte[]{1, 2, 3}, 0));
    }

    @Test
    public void testAsIntIfInputNotNull() {
        Assert.assertEquals(1, UtilAll.asInt("foo", 1));
        Assert.assertEquals(123, UtilAll.asInt("123", 1));
    }

    @Test
    public void testAsLongIfInputNotNull() {
        Assert.assertEquals(1l, UtilAll.asLong("foo", 1l));
        Assert.assertEquals(1556195014000l,
                UtilAll.asLong("1556195014000", 1l));
    }

    @Test
    public void testFormatDateIfInputNotNull() {
        Assert.assertEquals("2019-04-25 13:23:34",
                UtilAll.formatDate(
                        new Date(1556195014000l), "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    public void testParseDateIfInputNotNull() {
        Assert.assertNull(UtilAll.parseDate("2019-04-25 13:23:34", ""));

        Assert.assertEquals(new Date(1556195014000l),
                UtilAll.parseDate(
                        "2019-04-25 13:23:34", "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    public void testResponseCode2StringIfInputNotNull() {
        Assert.assertEquals("2", UtilAll.responseCode2String(2));
    }

    @Test
    public void testFrontStringAtLeastIfInputNotNull() {
        Assert.assertEquals("", UtilAll.frontStringAtLeast("", 2));
        Assert.assertEquals("fo", UtilAll.frontStringAtLeast("foo", 2));
    }

    @Test
    public void testIsBlankIfInputNullOrEmpty() {
        Assert.assertTrue(UtilAll.isBlank(""));
        Assert.assertTrue(UtilAll.isBlank(" "));
        Assert.assertTrue(UtilAll.isBlank(null));
    }

    @Test
    public void testIsBlankIfInputNotNull() {
        Assert.assertFalse(UtilAll.isBlank("foo"));
    }

    @Test
    public void testJstackIfNotNull() {
        Assert.assertNotNull(UtilAll.jstack());
    }

    @Test
    public void testIsInternalIPIfInputNotNull() {
        Assert.assertFalse(UtilAll.isInternalIP(new byte[]{1, 1, 1, 1}));

        Assert.assertTrue(UtilAll.isInternalIP(new byte[]{10, 1, 2, 3}));
        Assert.assertTrue(UtilAll.isInternalIP(
                new byte[]{(byte) 172, 16, 2, 3}));
        Assert.assertTrue(UtilAll.isInternalIP(
                new byte[]{(byte) 192, (byte) 168, 2, 3}));
    }

    @Test
    public void testIsInternalIPIfLengthLessThan4() {
        thrown.expect(RuntimeException.class);
        UtilAll.isInternalIP(new byte[0]);
        // Method is not expected to return due to exception thrown
    }

    @Test
    public void testIpToIPv4StrIfInputNotNull() {
        Assert.assertNull(UtilAll.ipToIPv4Str(new byte[]{1}));

        Assert.assertEquals("10.1.2.3",
                UtilAll.ipToIPv4Str(new byte[]{10, 1, 2, 3}));
        Assert.assertEquals("172.16.2.3",
                UtilAll.ipToIPv4Str(new byte[]{(byte) 172, 16, 2, 3}));
        Assert.assertEquals("192.168.2.3",
                UtilAll.ipToIPv4Str(
                        new byte[]{(byte) 192, (byte) 168, 2, 3}));
    }

    @Test
    public void testGetIP1IfCurrentIPNotNull() {
        InetAddress inetAddress = PowerMockito.mock(InetAddress.class);
        PowerMockito.when(inetAddress.getAddress())
                .thenReturn(new byte[]{10, 1, 2, 3});

        Assert.assertArrayEquals(new byte[]{-84, 17, 0, 1}, UtilAll.getIP());
    }

    @Test
    public void testGetIP2IfCurrentIPNotNull() {
        InetAddress inetAddress = PowerMockito.mock(InetAddress.class);
        PowerMockito.when(inetAddress.getAddress()).thenReturn(new byte[]{1});

        Assert.assertArrayEquals(new byte[]{-84, 17, 0, 1}, UtilAll.getIP());
    }

    @PrepareForTest({UtilAll.class, Enumeration.class, NetworkInterface.class})
    @Test
    public void testGetIPOutputRuntimeException() throws Exception {
        PowerMockito.mockStatic(NetworkInterface.class);
        Enumeration enumeration = PowerMockito.mock(Enumeration.class);
        PowerMockito.when(enumeration.hasMoreElements()).thenReturn(false);
        PowerMockito.doReturn(enumeration).when(NetworkInterface.class);
        NetworkInterface.getNetworkInterfaces();

        thrown.expect(RuntimeException.class);
        UtilAll.getIP();
        // Method is not expected to return due to exception thrown
    }
}
