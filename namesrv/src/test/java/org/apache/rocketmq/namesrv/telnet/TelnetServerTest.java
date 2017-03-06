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
package org.apache.rocketmq.namesrv.telnet;

import org.junit.Test;

public class TelnetServerTest {
    private static String addr = "192.168.185.172:9876";

    static {
        TelnetCommandUtil.setNamseSrvAddr(addr);
    }

    @Test
    public void testTelnetServer() {
        //System.out.println("hello");
        TelnetServer server = new TelnetServer(8090);
        try {
            server.start();
            System.out.println("start end");
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //for(;;);
    }

}
