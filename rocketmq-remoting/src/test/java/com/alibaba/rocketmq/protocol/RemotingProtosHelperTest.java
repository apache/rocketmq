/**
 * $Id: RemotingProtosHelperTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.protocol;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.alibaba.rocketmq.remoting.protocol.RemotingProtosHelper;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RemotingProtosHelperTest {
    @Test
    public void stringList2Bytes_bytes2StringList() throws InvalidProtocolBufferException {
        List<String> strs = new ArrayList<String>();
        strs.add("hello");
        strs.add("hi");
        strs.add("once");
        strs.add("there");
        strs.add("was");

        byte[] data = RemotingProtosHelper.stringList2Bytes(strs);
        assertTrue(data != null);

        // ≤‚ ‘∑¥–Ú¡–ªØ
        List<String> strsRead = RemotingProtosHelper.bytes2StringList(data);

        assertTrue(strsRead != null);
        assertTrue(strsRead.equals(strs));

        for (String str : strs) {
            System.out.println(str);
        }
    }

}
