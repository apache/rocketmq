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
package org.apache.rocketmq.proxy.service.transaction;

import java.util.List;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * no need to implements, because the channel of producer will put into the broker's producerManager
 */
public class LocalTransactionService extends AbstractTransactionService {

    protected final BrokerConfig brokerConfig;

    public LocalTransactionService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {

    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {

    }

    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {

    }

    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {

    }

    @Override
    protected String getBrokerNameByAddr(String brokerAddr) {
        return this.brokerConfig.getBrokerName();
    }
}
