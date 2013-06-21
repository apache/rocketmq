/**
 * $Id: RemotingProtosHelper.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.protocol;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.protobuf.InvalidProtocolBufferException;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.KVPair;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.KVPairList;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.StringList;


/**
 * 协议辅助类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class RemotingProtosHelper {
    /**
     * 序列化字符串列表
     */
    public static byte[] stringList2Bytes(final List<String> strs) {
        if (null == strs || strs.isEmpty()) {
            return null;
        }

        StringList.Builder builder = StringList.newBuilder();

        for (String str : strs) {
            builder.addName(str);
        }

        return builder.build().toByteArray();
    }


    /**
     * 反序列化字符串列表
     */
    public static List<String> bytes2StringList(final byte[] data) throws InvalidProtocolBufferException {
        if (null == data) {
            return null;
        }
        StringList stringList = StringList.parseFrom(data);
        return stringList.getNameList();
    }


    /**
     * 序列化名值对
     */
    public static byte[] hashMap2Bytes(final HashMap<Integer/* key */, String/* value */> nms) {
        if (null == nms || nms.isEmpty()) {
            return null;
        }

        KVPairList.Builder builder = KVPairList.newBuilder();

        Iterator<Entry<Integer, String>> it = nms.entrySet().iterator();
        for (int index = 0; it.hasNext(); index++) {
            Entry<Integer, String> entry = (Entry<Integer, String>) it.next();
            int key = entry.getKey();
            String val = entry.getValue();

            KVPair.Builder kvb = KVPair.newBuilder();
            kvb.setKey(key);
            kvb.setValue(val);
            builder.addFields(index, kvb.build());
        }

        return builder.build().toByteArray();
    }


    /**
     * 反序列化名值对
     * 
     * @throws InvalidProtocolBufferException
     */
    public static HashMap<Integer/* key */, String/* value */> bytes2HashMap(final byte[] data)
            throws InvalidProtocolBufferException {
        if (null == data) {
            return null;
        }

        HashMap<Integer/* key */, String/* value */> result =
                new HashMap<Integer/* key */, String/* value */>();

        KVPairList kvList = KVPairList.parseFrom(data);

        List<KVPair> kvList2 = kvList.getFieldsList();

        for (KVPair kv : kvList2) {
            result.put(kv.getKey(), kv.getValue());
        }

        return result;
    }
}
