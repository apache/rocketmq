/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.common;

import io.netty.channel.Channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.KVPair;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.KVPairList;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.NVPair;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.NVPairList;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.StringList;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * 通信层一些辅助方法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RemotingHelper {
    public static final String RemotingLogName = "RocketmqRemoting";


    /**
     * IP:PORT
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
        return isa;
    }


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


    /**
     * 序列化名值对
     */
    public static byte[] hashMapString2Bytes(final HashMap<String/* name */, String/* value */> nms) {
        if (null == nms || nms.isEmpty()) {
            return null;
        }

        NVPairList.Builder builder = NVPairList.newBuilder();

        Iterator<Entry<String, String>> it = nms.entrySet().iterator();
        for (int index = 0; it.hasNext(); index++) {
            Entry<String, String> entry = (Entry<String, String>) it.next();
            String key = entry.getKey();
            String val = entry.getValue();

            NVPair.Builder kvb = NVPair.newBuilder();
            kvb.setName(key);
            kvb.setValue(val);
            builder.addFields(index, kvb.build());
        }

        return builder.build().toByteArray();
    }


    /**
     * 序列化名值对
     */
    public static byte[] properties2Bytes(final Properties nms) {
        if (null == nms || nms.isEmpty()) {
            return null;
        }

        NVPairList.Builder builder = NVPairList.newBuilder();

        Set<Object> keyset = nms.keySet();
        int index = 0;
        for (Object object : keyset) {
            String key = object.toString();
            String val = nms.getProperty(key);

            NVPair.Builder kvb = NVPair.newBuilder();
            kvb.setName(key);
            kvb.setValue(val);
            builder.addFields(index++, kvb.build());
        }

        return builder.build().toByteArray();
    }


    /**
     * 反序列化名值对
     * 
     * @throws InvalidProtocolBufferException
     */
    public static HashMap<String/* name */, String/* value */> bytes2HashMapString(final byte[] data)
            throws InvalidProtocolBufferException {
        if (null == data) {
            return null;
        }

        HashMap<String/* name */, String/* value */> result =
                new HashMap<String/* name */, String/* value */>();

        NVPairList ps = NVPairList.parseFrom(data);

        List<NVPair> ps2 = ps.getFieldsList();

        for (NVPair kv : ps2) {
            result.put(kv.getName(), kv.getValue());
        }

        return result;
    }


    /**
     * 短连接调用 TODO
     */
    public static RemotingCommand invokeSync(final String addr, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        long beginTime = System.currentTimeMillis();
        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
        SocketChannel socketChannel = RemotingUtil.connect(socketAddress);
        if (socketChannel != null) {
            boolean sendRequestOK = false;

            try {
                // 使用阻塞模式
                socketChannel.configureBlocking(true);
                /*
                 * FIXME The read methods in SocketChannel (and DatagramChannel)
                 * do notsupport timeouts
                 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
                 */
                socketChannel.socket().setSoTimeout((int) timeoutMillis);

                // 发送数据
                ByteBuffer byteBufferRequest = request.encode();
                while (byteBufferRequest.hasRemaining()) {
                    int length = socketChannel.write(byteBufferRequest);
                    if (length > 0) {
                        if (byteBufferRequest.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 发送请求超时
                                throw new RemotingSendRequestException(addr);
                            }
                        }
                    }
                    else {
                        throw new RemotingSendRequestException(addr);
                    }

                    // 比较土
                    Thread.sleep(1);
                }

                sendRequestOK = true;

                // 接收应答 SIZE
                ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
                while (byteBufferSize.hasRemaining()) {
                    int length = socketChannel.read(byteBufferSize);
                    if (length > 0) {
                        if (byteBufferSize.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 接收应答超时
                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    }
                    else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    // 比较土
                    Thread.sleep(1);
                }

                // 接收应答 BODY
                int size = byteBufferSize.getInt(0);
                ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
                while (byteBufferBody.hasRemaining()) {
                    int length = socketChannel.read(byteBufferBody);
                    if (length > 0) {
                        if (byteBufferBody.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 接收应答超时
                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    }
                    else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    // 比较土
                    Thread.sleep(1);
                }

                // 对应答数据解码
                byteBufferBody.flip();
                return RemotingCommand.decode(byteBufferBody);
            }
            catch (IOException e) {
                e.printStackTrace();

                if (sendRequestOK) {
                    throw new RemotingTimeoutException(addr, timeoutMillis);
                }
                else {
                    throw new RemotingSendRequestException(addr);
                }
            }
            finally {
                try {
                    socketChannel.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            throw new RemotingConnectException(addr);
        }
    }


    public static String parseChannelRemoteAddr(final Channel channel) {
        if(null == channel){
            return "";
        }
        final SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            return addr.substring(1);
        }

        return "";
    }


    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            final String addr = socketAddress.toString();

            if (addr.length() > 0) {
                return addr.substring(1);
            }
        }
        return "";
    }
}
