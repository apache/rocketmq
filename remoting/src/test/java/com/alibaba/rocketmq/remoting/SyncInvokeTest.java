/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * $Id: SyncInvokeTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr
 */
public class SyncInvokeTest {
    @Test
    public void test_RPC_Sync() throws Exception {
        RemotingServer server = NettyRPCTest.createRemotingServer();
        RemotingClient client = NettyRPCTest.createRemotingClient();

        for (int i = 0; i < 100; i++) {
            try {
                RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
                RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
                System.out.println(i + "\t" + "invoke result = " + response);
                assertTrue(response != null);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }
}
