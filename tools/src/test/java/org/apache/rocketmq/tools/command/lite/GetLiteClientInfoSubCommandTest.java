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

package org.apache.rocketmq.tools.command.lite;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.remoting.protocol.body.GetLiteClientInfoResponseBody;
import org.junit.Test;

public class GetLiteClientInfoSubCommandTest {

    private GetLiteClientInfoResponseBody mockResponseBody() {
        GetLiteClientInfoResponseBody responseBody = new GetLiteClientInfoResponseBody();
        responseBody.setParentTopic("testParentTopic");
        responseBody.setGroup("testGroup");
        responseBody.setClientId("testClientId");
        responseBody.setLastAccessTime(System.currentTimeMillis());
        responseBody.setLastConsumeTime(System.currentTimeMillis());
        responseBody.setLiteTopicCount(5);
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add("liteTopic1");
        liteTopicSet.add("liteTopic2");
        responseBody.setLiteTopicSet(liteTopicSet);
        return responseBody;
    }

    @Test
    public void testPrint() {
        GetLiteClientInfoResponseBody responseBody = mockResponseBody();
        GetLiteClientInfoSubCommand.printHeader();
        GetLiteClientInfoSubCommand.printRow(responseBody, "brokerName1", true);
        GetLiteClientInfoSubCommand.printRow(responseBody, "brokerName2", true);
        GetLiteClientInfoSubCommand.printHeader();
    }
}
