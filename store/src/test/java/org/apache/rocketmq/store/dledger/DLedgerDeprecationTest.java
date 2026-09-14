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
package org.apache.rocketmq.store.dledger;

import java.lang.reflect.Method;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;

public class DLedgerDeprecationTest {

    @Test
    public void testDLedgerCommitLogIsDeprecated() {
        Assert.assertTrue(DLedgerCommitLog.class.isAnnotationPresent(Deprecated.class));
    }

    @Test
    public void testDLedgerMessageStoreConfigAccessorsAreDeprecated() throws Exception {
        assertDeprecated(MessageStoreConfig.class.getMethod("getStorePathDLedgerCommitLog"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setStorePathDLedgerCommitLog", String.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("getdLegerGroup"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setdLegerGroup", String.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("getdLegerPeers"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setdLegerPeers", String.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("getdLegerSelfId"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setdLegerSelfId", String.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("isEnableDLegerCommitLog"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setEnableDLegerCommitLog", boolean.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("getPreferredLeaderId"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setPreferredLeaderId", String.class));
        assertDeprecated(MessageStoreConfig.class.getMethod("isEnableBatchPush"));
        assertDeprecated(MessageStoreConfig.class.getMethod("setEnableBatchPush", boolean.class));
    }

    private void assertDeprecated(Method method) {
        Assert.assertTrue(method.getName() + " should be deprecated",
            method.isAnnotationPresent(Deprecated.class));
    }
}
