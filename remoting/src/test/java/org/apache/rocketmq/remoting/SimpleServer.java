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

package org.apache.rocketmq.remoting;

import static org.apache.rocketmq.remoting.TlsTest.getCertsPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_AUTHSERVER;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_AUTHCLIENT;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_MODE;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_NEED_CLIENT_AUTH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_TEST_MODE_ENABLE;

public class SimpleServer {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty(TLS_SERVER_MODE, "enforcing");
        System.setProperty(TLS_TEST_MODE_ENABLE, "false");
        System.setProperty(TLS_SERVER_NEED_CLIENT_AUTH, "require");

        System.setProperty(TLS_SERVER_KEYPATH, getCertsPath("server.key"));
        System.setProperty(TLS_SERVER_CERTPATH, getCertsPath("server.pem"));
        System.setProperty(TLS_SERVER_AUTHCLIENT, "true");
        System.setProperty(TLS_SERVER_TRUSTCERTPATH, getCertsPath("ca.pem"));

        System.setProperty(TLS_CLIENT_KEYPATH, getCertsPath("client.key"));
        System.setProperty(TLS_CLIENT_CERTPATH, getCertsPath("client.pem"));
        System.setProperty(TLS_CLIENT_AUTHSERVER, "true");
        System.setProperty(TLS_CLIENT_TRUSTCERTPATH, getCertsPath("ca.pem"));

        RemotingServer remotingServer = RemotingServerTest.createRemotingServer();

    }
}
