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
package org.apache.rocketmq.tieredstore.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreUtilTest {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    private static final String TIERED_STORE_PATH = "tiered_store_test";

    public static String getRandomStorePath() {
        return Paths.get(System.getProperty("user.home"), TIERED_STORE_PATH,
            UUID.randomUUID().toString().replace("-", "").toUpperCase().substring(0, 16)).toString();
    }

    public static void deleteStoreDirectory(String storePath) {
        try {
            FileUtils.deleteDirectory(new File(storePath));
        } catch (IOException e) {
            log.error("Delete store directory failed, filePath: {}", storePath, e);
        }
    }

    @Test
    @SuppressWarnings("DoubleBraceInitialization")
    public void toHumanReadableTest() {
        Map<Long, String> capacityTable = new HashMap<Long, String>() {
            {
                put(-1L, "-1");
                put(0L, "0B");
                put(1023L, "1023B");
                put(1024L, "1KB");
                put(12_345L, "12.06KB");
                put(10_123_456L, "9.65MB");
                put(10_123_456_798L, "9.43GB");
                put(123 * 1024L * 1024L * 1024L * 1024L, "123TB");
                put(123 * 1024L * 1024L * 1024L * 1024L * 1024L, "123PB");
                put(1_777_777_777_777_777_777L, "1.54EB");
            }
        };
        capacityTable.forEach((in, expected) ->
            Assert.assertEquals(expected, MessageStoreUtil.toHumanReadable(in)));
    }

    @Test
    public void getHashTest() {
        Assert.assertEquals("161c08ff", MessageStoreUtil.getHash("TieredStorageDailyTest"));
    }

    @Test
    public void filePathTest() {
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName("BrokerName");
        mq.setTopic("topicName");
        mq.setQueueId(2);
        Assert.assertEquals("BrokerName/topicName/2", MessageStoreUtil.toFilePath(mq));
    }

    @Test
    public void offset2FileNameTest() {
        Assert.assertEquals("cfcd208400000000000000000000", MessageStoreUtil.offset2FileName(0));
        Assert.assertEquals("b10da56800000000004294937144", MessageStoreUtil.offset2FileName(4294937144L));
    }

    @Test
    public void fileName2OffsetTest() {
        Assert.assertEquals(0, MessageStoreUtil.fileName2Offset("cfcd208400000000000000000000"));
        Assert.assertEquals(4294937144L, MessageStoreUtil.fileName2Offset("b10da56800000000004294937144"));
    }

    @Test
    public void indexServicePathTest() {
        Assert.assertEquals("brokerName/rmq_sys_INDEX/0", MessageStoreUtil.getIndexFilePath("brokerName"));
    }
}
