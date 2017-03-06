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

public class CommandTest {
    private static String addr = "192.168.185.172:9876";

    static {
        TelnetCommandUtil.setNamseSrvAddr(addr);
    }

    //@Test
    public void topicStatusTest() {
        //System.out.println("hello");
        //TelnetCommandUtil.setNamseSrvAddr(addr);
        String topicStr = "topicStatus -t TestTopic";
        String str = TelnetCommandUtil.doCmd(topicStr);
        //System.out.println(str);
    }

    @Test
    public void producerConTest() {
        String producerStr = "producerConnection -t 78zzInfo -g 78zzInfoSendGroup ";
        String str = TelnetCommandUtil.doCmd(producerStr);
        //System.out.println(str);
    }

    //@Test
    public void consumerConTest() {
        String consumerCon = "consumerConnection  -g 78zzArbitrationSubGrouphh";
        String str = TelnetCommandUtil.doCmd(consumerCon);
        //System.out.println(str);
    }

    //@Test
    public void consumerProgressTest() {
        String conStr = "consumerProgress  -g 78zzArbitrationSubGroup";
        String str = TelnetCommandUtil.doCmd(conStr);
        //System.out.println(str);
    }

}
