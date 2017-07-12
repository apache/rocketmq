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

package org.apache.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * BrokerData tests.
 */
public class BrokerDataTest {
    private static BrokerData brokerData;

    @BeforeClass
    public static void prepare() {
        brokerData = new BrokerData("testCluster", "testBroker",
            new HashMap<Long, String>() {{
                put(1L, "addr1");
                put(2L, "addr2");
                put(3L, "addr3");
            }});
    }

    @Test
    public void selectBrokerAddr() throws Exception {
        List<String> selectedAddr = new ArrayList<String>();

        for (int i = 0; i < 5; i++)
            selectedAddr.add(brokerData.selectBrokerAddr());

        List<String> firstElemList = new ArrayList<String>();

        for (int i = 0; i < 5; i++)
            firstElemList.add(selectedAddr.get(0));

        Assert.assertFalse("Contains same addresses", selectedAddr.equals(firstElemList));
    }
}
