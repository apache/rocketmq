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
package org.apache.rocketmq.common.sysflag;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageSysFlagTest {
    private static final int NON_TRANSACTION_FLAGS = MessageSysFlag.COMPRESSED_FLAG
        | MessageSysFlag.BORNHOST_V6_FLAG | MessageSysFlag.COMPRESSION_ZSTD_TYPE;

    @Test
    public void testGetTransactionValueIgnoresOtherFlags() {
        assertThat(MessageSysFlag.getTransactionValue(NON_TRANSACTION_FLAGS))
            .isEqualTo(MessageSysFlag.TRANSACTION_NOT_TYPE);
        assertThat(MessageSysFlag.getTransactionValue(NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_PREPARED_TYPE))
            .isEqualTo(MessageSysFlag.TRANSACTION_PREPARED_TYPE);
        assertThat(MessageSysFlag.getTransactionValue(NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_COMMIT_TYPE))
            .isEqualTo(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
        assertThat(MessageSysFlag.getTransactionValue(NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_ROLLBACK_TYPE))
            .isEqualTo(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
    }

    @Test
    public void testResetTransactionValuePreservesOtherFlags() {
        int preparedFlag = NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        int commitFlag = NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_COMMIT_TYPE;
        int rollbackFlag = NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;

        assertThat(MessageSysFlag.resetTransactionValue(rollbackFlag, MessageSysFlag.TRANSACTION_PREPARED_TYPE))
            .isEqualTo(preparedFlag);
        assertThat(MessageSysFlag.resetTransactionValue(preparedFlag, MessageSysFlag.TRANSACTION_COMMIT_TYPE))
            .isEqualTo(commitFlag);
        assertThat(MessageSysFlag.resetTransactionValue(commitFlag, MessageSysFlag.TRANSACTION_ROLLBACK_TYPE))
            .isEqualTo(rollbackFlag);
    }

    @Test
    public void testResetTransactionValueToNotTypeClearsTransactionBits() {
        int rollbackFlag = NON_TRANSACTION_FLAGS | MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;

        assertThat(MessageSysFlag.resetTransactionValue(rollbackFlag, MessageSysFlag.TRANSACTION_NOT_TYPE))
            .isEqualTo(NON_TRANSACTION_FLAGS);
    }
}
