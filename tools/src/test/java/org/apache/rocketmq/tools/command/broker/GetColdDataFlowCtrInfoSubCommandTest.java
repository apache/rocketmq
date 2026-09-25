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
package org.apache.rocketmq.tools.command.broker;

import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetColdDataFlowCtrInfoSubCommandTest {

    @Test
    public void testPrintsEntriesWhoseGroupNameContainsPercent() throws Exception {
        MQAdminExt admin = mock(MQAdminExt.class);
        // '%' is a legal character in consumer group and topic names, e.g. "%RETRY%" prefixed ones
        String payload = "{\"runtimeTable\":{\"order%group\":{\"coldAcc\":10,"
            + "\"lastColdReadTimeMills\":1700000000000,\"createTimeMills\":1600000000000}},"
            + "\"configTable\":{},\"cgColdReadThreshold\":100,\"globalColdReadThreshold\":1000,\"globalAcc\":5}";
        when(admin.getColdDataFlowCtrInfo("127.0.0.1:10911")).thenReturn(payload);

        PrintStream originalOut = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8.name()));
        try {
            new GetColdDataFlowCtrInfoSubCommand().getAndPrint(admin, "prefix", "127.0.0.1:10911");
        } finally {
            System.setOut(originalOut);
        }

        assertThat(buffer.toString()).contains("order%group");
    }
}
