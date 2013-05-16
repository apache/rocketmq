/**
 * $Id: RemotingProtosHelperTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.protocol;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtosHelper;


/**
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
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

        // 测试反序列化
        List<String> strsRead = RemotingProtosHelper.bytes2StringList(data);

        assertTrue(strsRead != null);
        assertTrue(strsRead.equals(strs));

        for (String str : strs) {
            System.out.println(str);
        }
    }


    @Test
    public void hashMap2Bytes_bytes2HashMap() throws InvalidProtocolBufferException {
        HashMap<Integer/* key */, String/* value */> nmsWrite = new HashMap<Integer/* key */, String/* value */>();
        nmsWrite.put(1, "hello");
        nmsWrite.put(2, "hi");
        nmsWrite.put(4, "nice");
        nmsWrite.put(6, "nice");
        nmsWrite.put(8, "nice");

        // 测试序列化
        byte[] data = RemotingProtosHelper.hashMap2Bytes(nmsWrite);
        assertTrue(data != null);

        // 测试反序列化
        HashMap<Integer/* key */, String/* value */> nmsRead = RemotingProtosHelper.bytes2HashMap(data);

        assertTrue(nmsRead != null);
        assertTrue(nmsRead.equals(nmsWrite));

        Iterator<Entry<Integer, String>> it = nmsRead.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, String> entry = (Entry<Integer, String>) it.next();
            Integer key = entry.getKey();
            String val = entry.getValue();
            System.out.println(key + "  " + val);
        }
    }
}
