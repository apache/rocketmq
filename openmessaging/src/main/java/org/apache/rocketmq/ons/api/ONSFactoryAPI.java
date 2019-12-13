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
package org.apache.rocketmq.ons.api;

import io.openmessaging.api.Consumer;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.Producer;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.order.OrderConsumer;
import io.openmessaging.api.order.OrderProducer;
import io.openmessaging.api.transaction.LocalTransactionChecker;
import io.openmessaging.api.transaction.TransactionProducer;
import java.util.Properties;

/**
 * {@link MessagingAccessPoint} is recommended.
 */
@Deprecated
public interface ONSFactoryAPI {

    Producer createProducer(final Properties properties);

    Consumer createConsumer(final Properties properties);

    BatchConsumer createBatchConsumer(final Properties properties);

    OrderProducer createOrderProducer(final Properties properties);

    OrderConsumer createOrderedConsumer(final Properties properties);

    TransactionProducer createTransactionProducer(final Properties properties,
        final LocalTransactionChecker checker);

    PullConsumer createPullConsumer(final Properties properties);
}
