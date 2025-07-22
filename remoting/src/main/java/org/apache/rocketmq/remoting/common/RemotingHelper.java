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
package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class RemotingHelper {
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String DEFAULT_CIDR_ALL = "0.0.0.0/0";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    public static final Map<Integer, String> REQUEST_CODE_MAP = new HashMap<>(256) ;

    public static final Map<Integer, String> RESPONSE_CODE_MAP = new HashMap<>(128);

    static {
        try {
            Field[] f = RequestCode.class.getFields();
            for (Field field : f) {
                if (field.getType() == int.class) {
                    REQUEST_CODE_MAP.put((int) field.get(null), field.getName().toLowerCase());
                }
            }
        } catch (IllegalAccessException ignore) {
        }

        try {
            Field[] f = ResponseCode.class.getFields();
            for (Field field : f) {
                if (field.getType() == int.class) {
                    RESPONSE_CODE_MAP.put((int) field.get(null), field.getName().toLowerCase());
                }
            }
        } catch (IllegalAccessException ignore) {
        }
    }

    public static <T> T getAttributeValue(AttributeKey<T> key, final Channel channel) {
        if (channel.hasAttr(key)) {
            Attribute<T> attribute = channel.attr(key);
            return attribute.get();
        }
        return null;
    }

    public static <T> void setPropertyToAttr(final Channel channel, AttributeKey<T> attributeKey, T value) {
        if (channel == null) {
            return;
        }
        channel.attr(attributeKey).set(value);
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
        return isa;
    }

    public static RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException {
        long beginTime = System.currentTimeMillis();
        SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
        SocketChannel socketChannel = connect(socketAddress);
        if (socketChannel != null) {
            boolean sendRequestOK = false;

            try {

                socketChannel.configureBlocking(true);

                //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
                socketChannel.socket().setSoTimeout((int) timeoutMillis);

                ByteBuffer byteBufferRequest = request.encode();
                while (byteBufferRequest.hasRemaining()) {
                    int length = socketChannel.write(byteBufferRequest);
                    if (length > 0) {
                        if (byteBufferRequest.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingSendRequestException(addr);
                            }
                        }
                    } else {
                        throw new RemotingSendRequestException(addr);
                    }

                    Thread.sleep(1);
                }

                sendRequestOK = true;

                ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
                while (byteBufferSize.hasRemaining()) {
                    int length = socketChannel.read(byteBufferSize);
                    if (length > 0) {
                        if (byteBufferSize.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }

                int size = byteBufferSize.getInt(0);
                ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
                while (byteBufferBody.hasRemaining()) {
                    int length = socketChannel.read(byteBufferBody);
                    if (length > 0) {
                        if (byteBufferBody.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }

                byteBufferBody.flip();
                return RemotingCommand.decode(byteBufferBody);
            } catch (IOException e) {
                log.error("invokeSync failure", e);

                if (sendRequestOK) {
                    throw new RemotingTimeoutException(addr, timeoutMillis);
                } else {
                    throw new RemotingSendRequestException(addr);
                }
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new RemotingConnectException(addr);
        }
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        String addr = getProxyProtocolAddress(channel);
        if (StringUtils.isNotBlank(addr)) {
            return addr;
        }
        Attribute<String> att = channel.attr(AttributeKeys.REMOTE_ADDR_KEY);
        if (att == null) {
            // mocked in unit test
            return parseChannelRemoteAddr0(channel);
        }
        addr = att.get();
        if (addr == null) {
            addr = parseChannelRemoteAddr0(channel);
            att.set(addr);
        }
        return addr;
    }

    private static String getProxyProtocolAddress(Channel channel) {
        if (!channel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)) {
            return null;
        }
        String proxyProtocolAddr = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_ADDR, channel);
        String proxyProtocolPort = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_PORT, channel);
        if (StringUtils.isBlank(proxyProtocolAddr) || proxyProtocolPort == null) {
            return null;
        }
        return proxyProtocolAddr + ":" + proxyProtocolPort;
    }

    private static String parseChannelRemoteAddr0(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    public static String parseChannelLocalAddr(final Channel channel) {
        SocketAddress remote = channel.localAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    public static String parseHostFromAddress(String address) {
        if (address == null) {
            return "";
        }

        String[] addressSplits = address.split(":");
        if (addressSplits.length < 1) {
            return "";
        }

        return addressSplits[0];
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            // Default toString of InetSocketAddress is "hostName/IP:port"
            final String addr = socketAddress.toString();
            int index = addr.lastIndexOf("/");
            return (index != -1) ? addr.substring(index + 1) : addr;
        }
        return "";
    }

    public static Integer parseSocketAddressPort(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) socketAddress).getPort();
        }
        return -1;
    }

    public static int ipToInt(String ip) {
        String[] ips = ip.split("\\.");
        return (Integer.parseInt(ips[0]) << 24)
            | (Integer.parseInt(ips[1]) << 16)
            | (Integer.parseInt(ips[2]) << 8)
            | Integer.parseInt(ips[3]);
    }

    public static boolean ipInCIDR(String ip, String cidr) {
        int ipAddr = ipToInt(ip);
        String[] cidrArr = cidr.split("/");
        int netId = Integer.parseInt(cidrArr[1]);
        int mask = 0xFFFFFFFF << (32 - netId);
        int cidrIpAddr = ipToInt(cidrArr[0]);

        return (ipAddr & mask) == (cidrIpAddr & mask);
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);
            if (NettySystemConfig.socketSndbufSize > 0) {
                sc.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
            }
            if (NettySystemConfig.socketRcvbufSize > 0) {
                sc.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
            }
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        if ("".equals(addrRemote)) {
            channel.close();
        } else {
            channel.close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess());
                }
            });
        }
    }

    public static CompletableFuture<Void> convertChannelFutureToCompletableFuture(ChannelFuture channelFuture) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                completableFuture.complete(null);
            } else {
                completableFuture.completeExceptionally(new RemotingConnectException(channelFuture.channel().remoteAddress().toString(), future.cause()));
            }
        });
        return completableFuture;
    }

    public static String getRequestCodeDesc(int code) {
        return REQUEST_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }

    public static String getResponseCodeDesc(int code) {
        return RESPONSE_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }
}
