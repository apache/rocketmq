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
package org.apache.rocketmq.proxy.client;

import org.apache.rocketmq.proxy.client.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;

public class ClientManager extends AbstractStartAndShutdown {

    private final ClientFactory clientFactory;
    private final DefaultClient defaultClient;
    private final ProducerClient producerClient;
    private final ReadConsumerClient readConsumerClient;
    private final WriteConsumerClient writeConsumerClient;

    private final TopicRouteCache topicRouteCache;

    public ClientManager(TransactionStateChecker transactionStateChecker) {
        this.clientFactory = new ClientFactory(transactionStateChecker);
        this.defaultClient = new DefaultClient(this.clientFactory);
        this.producerClient = new ProducerClient(this.clientFactory);
        this.readConsumerClient = new ReadConsumerClient(this.clientFactory);
        this.writeConsumerClient = new WriteConsumerClient(this.clientFactory);

        this.topicRouteCache = new TopicRouteCache(this.defaultClient);

        this.appendStartAndShutdown(this.clientFactory);
        this.appendStartAndShutdown(this.defaultClient);
        this.appendStartAndShutdown(this.producerClient);
        this.appendStartAndShutdown(this.readConsumerClient);
        this.appendStartAndShutdown(this.writeConsumerClient);
    }

    public DefaultClient getDefaultClient() {
        return defaultClient;
    }

    public ProducerClient getProducerClient() {
        return producerClient;
    }

    public ReadConsumerClient getReadConsumerClient() {
        return readConsumerClient;
    }

    public WriteConsumerClient getWriteConsumerClient() {
        return writeConsumerClient;
    }

    public TopicRouteCache getTopicRouteCache() {
        return topicRouteCache;
    }
}
