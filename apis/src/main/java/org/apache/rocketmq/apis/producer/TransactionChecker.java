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

package org.apache.rocketmq.apis.producer;

import org.apache.rocketmq.apis.message.MessageView;

/**
 * Used to determine {@link TransactionResolution} when {@link Transaction} is not committed or roll-backed in time.
 * {@link Transaction#commit()} and {@link Transaction#rollback()} does not promise that it would be applied
 * successfully, so that checker here is necessary.
 *
 * <p>If {@link TransactionChecker#check(MessageView)} returns {@link TransactionResolution#UNKNOWN} or exception
 * raised during the invocation of {@link TransactionChecker#check(MessageView)}, the examination from server will be
 * performed periodically.
 */
public interface TransactionChecker {
    /**
     * Server will solve the suspended transactional message by this method.
     *
     * <p>If exception was thrown in this method, which equals {@link TransactionResolution#UNKNOWN} is returned.
     *
     * @param messageView message to determine {@link TransactionResolution}.
     * @return the transaction resolution.
     */
    TransactionResolution check(MessageView messageView);
}
