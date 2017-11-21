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

package org.apache.rocketmq.remoting.netty;

import org.apache.rocketmq.remoting.common.SslMode;

public class NettySystemConfig {
    public static final String COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
        "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE =
        "com.rocketmq.remoting.socket.sndbuf.size";
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE =
        "com.rocketmq.remoting.socket.rcvbuf.size";
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientOnewaySemaphoreValue";

    public static final String ORG_APACHE_ROCKETMQ_REMOTING_SSL_MODE = //
        "org.apache.rocketmq.remoting.ssl.mode";

    public static final String ORG_APACHE_ROCKETMQ_REMOTING_SSL_CONFIG_FILE = //
        "org.apache.rocketmq.remoting.ssl.config.file";

    public static final boolean NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE = //
        Boolean.parseBoolean(System.getProperty(COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE, "false"));
    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE = //
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
    public static int socketSndbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "65535"));
    public static int socketRcvbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "65535"));

    /**
     * For server, three SSL modes are supported: disabled, permissive and enforcing.
     * <ol>
     *     <li><strong>disabled:</strong> SSL is not supported; any incoming SSL handshake will be rejected, causing connection closed.</li>
     *     <li><strong>permissive:</strong> SSL is optional, aka, server in this mode can serve client connections with or without SSL;</li>
     *     <li><strong>enforcing:</strong> SSL is required, aka, non SSL connection will be rejected.</li>
     * </ol>
     */
    public static SslMode sslMode = //
        SslMode.parse(System.getProperty(ORG_APACHE_ROCKETMQ_REMOTING_SSL_MODE, "permissive"));

    public static String sslConfigFile = //
        System.getProperty(ORG_APACHE_ROCKETMQ_REMOTING_SSL_CONFIG_FILE, "/etc/rocketmq/ssl.properties");
}
