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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MessageClientIDSetterTest {

    @Test
    public void testGetNearlyTimeFromID() throws ParseException {

        MessageClientIDSetter messageClientIDSetter = new MessageClientIDSetter();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //2017-03-07 09:04:00
        String message1 = "AC1F0B01327B5B2133B120D8340B3D7D";
        Date actualDate1 = messageClientIDSetter.getNearlyTimeFromID(message1);
        //2017-03-07 15:11:13
        String message2 = "AC1F0B01696C5B2133B1222866B409C9";
        Date actualDate2 = messageClientIDSetter.getNearlyTimeFromID(message2);

        Date expectedDate1 = sdf.parse("2017-03-07 09:04:00");
        Date expectedDate2 = sdf.parse("2017-03-07 15:11:13");

        Assert.assertEquals(expectedDate1.toString(), actualDate1.toString());
        Assert.assertEquals(expectedDate2.toString(), actualDate2.toString());
    }
}