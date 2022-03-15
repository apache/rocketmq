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

import org.apache.rocketmq.proxy.client.factory.ForwardClientFactory;
import org.apache.rocketmq.proxy.client.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;

public class ForwardClientManager extends AbstractStartAndShutdown {
    private final ForwardClientFactory forwardClientFactory;
    private final DefaultForwardClient defaultForwardClient;
    private final ForwardProducer forwardProducer;
    private final ForwardReadConsumer forwardReadConsumer;
    private final ForwardWriteConsumer forwardWriteConsumer;

    private final TopicRouteCache topicRouteCache;

    public ForwardClientManager(TransactionStateChecker transactionStateChecker) {
        this.forwardClientFactory = new ForwardClientFactory(transactionStateChecker);
        this.defaultForwardClient = new DefaultForwardClient(this.forwardClientFactory);
        this.forwardProducer = new ForwardProducer(this.forwardClientFactory);
        this.forwardReadConsumer = new ForwardReadConsumer(this.forwardClientFactory);
        this.forwardWriteConsumer = new ForwardWriteConsumer(this.forwardClientFactory);

        this.topicRouteCache = new TopicRouteCache(this.defaultForwardClient);

        this.appendStartAndShutdown(this.forwardClientFactory);
        this.appendStartAndShutdown(this.defaultForwardClient);
        this.appendStartAndShutdown(this.forwardProducer);
        this.appendStartAndShutdown(this.forwardReadConsumer);
        this.appendStartAndShutdown(this.forwardWriteConsumer);
    }

    public ForwardClientFactory getForwardClientFactory() {
        return forwardClientFactory;
    }

    public DefaultForwardClient getDefaultForwardClient() {
        return defaultForwardClient;
    }

    public ForwardProducer getForwardProducer() {
        return forwardProducer;
    }

    public ForwardReadConsumer getForwardReadConsumer() {
        return forwardReadConsumer;
    }

    public ForwardWriteConsumer getForwardWriteConsumer() {
        return forwardWriteConsumer;
    }

    public TopicRouteCache getTopicRouteCache() {
        return topicRouteCache;
    }
}
