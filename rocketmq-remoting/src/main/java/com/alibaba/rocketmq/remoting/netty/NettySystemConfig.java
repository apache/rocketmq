package com.alibaba.rocketmq.remoting.netty;

public class NettySystemConfig {
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable =
            "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static boolean NettyPooledByteBufAllocatorEnable = //
            Boolean
                .parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "false"));

    public static final String SystemPropertySocketSndbufSize = //
            "com.rocketmq.remoting.socket.sndbuf.size";
    public static int SocketSndbufSize = //
            Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "65535"));

    public static final String SystemPropertySocketRcvbufSize = //
            "com.rocketmq.remoting.socket.rcvbuf.size";
    public static int SocketRcvbufSize = //
            Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "65535"));
}
