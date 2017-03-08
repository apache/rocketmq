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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class MessageClientIDSetterTest {

    @Test
    public void testGetNearlyTimeFromID() {

        MessageClientIDSetter messageClientIDSetter = new MessageClientIDSetter();
        //2017-03-07 09:04:00
        String message1 = "AC1F0B01327B5B2133B120D8340B3D7D";
        Date date1 = messageClientIDSetter.getNearlyTimeFromID(message1);
        //2017-03-07 15:11:13
        String message2 = "AC1F0B01696C5B2133B1222866B409C9";
        Date date2 = messageClientIDSetter.getNearlyTimeFromID(message2);

        Assert.assertEquals("Tue Mar 07 09:04:00 GMT+08:00 2017", date1.toString());
        Assert.assertEquals("Tue Mar 07 15:11:13 GMT+08:00 2017", date2.toString());
    }
}