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

package org.apache.rocketmq.test.container;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {
    private boolean shouldReturnUnknownState = false;



    public TransactionListenerImpl(boolean shouldReturnUnknownState) {
        this.shouldReturnUnknownState = shouldReturnUnknownState;
    }

    public void setShouldReturnUnknownState(boolean shouldReturnUnknownState) {
        this.shouldReturnUnknownState = shouldReturnUnknownState;
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        if (shouldReturnUnknownState) {
            return LocalTransactionState.UNKNOW;
        } else {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        if (shouldReturnUnknownState) {
            return LocalTransactionState.UNKNOW;
        } else {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
