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

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 通信层一些辅助方法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class RemotingHelper {
    public static final String RemotingLogName = "RocketmqRemoting";


    public static String exceptionSimpleDesc(final Throwable e) {
        StringBuffer sb = new StringBuffer();
        if (e != null) {
            sb.append(e.toString());

            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement elment = stackTrace[0];
                sb.append(", ");
                sb.append(elment.toString());
            }
        }

        return sb.toString();
    }


    /**
     * IP:PORT
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
        return isa;
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
        if (null == channel) {
            return "";
        }
        final SocketAddress remote = channel.remoteAddress();
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


    public static String parseChannelRemoteName(final Channel channel) {
        if (null == channel) {
            return "";
        }
        final InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
        if (remote != null) {
            return remote.getAddress().getHostName();
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


    public static String parseSocketAddressName(SocketAddress socketAddress) {

        final InetSocketAddress addrs = (InetSocketAddress) socketAddress;
        if (addrs != null) {
            return addrs.getAddress().getHostName();
        }
        return "";
    }

}
