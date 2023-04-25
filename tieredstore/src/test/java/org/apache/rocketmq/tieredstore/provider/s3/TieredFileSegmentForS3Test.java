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

package org.apache.rocketmq.tieredstore.provider.s3;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegmentBaseTest;
import org.junit.After;
import org.junit.Before;

public class TieredFileSegmentForS3Test extends TieredFileSegmentBaseTest {

    private MockS3TestBase mockS3TestBase = new MockS3TestBase();

    private static final TieredMessageStoreConfig CONFIG = new TieredMessageStoreConfig();

    static {
        CONFIG.setBrokerClusterName("test-cluster");
        CONFIG.setBrokerName("test-broker");
        CONFIG.setObjectStoreRegion("ap-northeast-1");
        CONFIG.setObjectStoreBucket("rocketmq-lcy");
        CONFIG.setObjectStoreAccessKey("");
        CONFIG.setObjectStoreSecretKey("");
    }

    public TieredFileSegment createFileSegment(TieredFileSegment.FileSegmentType fileType) {
        return new S3FileSegment(fileType, new MessageQueue("TieredFileSegmentTest", CONFIG.getBrokerName(), 0),
            baseOffset, CONFIG);
    }

    @Before
    public void setUp() {
        mockS3TestBase.startMockedS3();
    }

    @After
    public void tearDown() {
        mockS3TestBase.clearMockS3Data();
    }
}
