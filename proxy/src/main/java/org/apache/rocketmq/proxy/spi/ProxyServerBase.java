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

package org.apache.rocketmq.proxy.spi;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

import java.util.List;

public abstract class ProxyServerBase implements ProxyServer {

    private BrokerController brokerController;
    private List<StartAndShutdown> startAndShutdowns;
    private MessagingProcessor messagingProcessor;

    @Override
    public List<StartAndShutdown> getStartAndShutdowns() {
        return startAndShutdowns;
    }

    @Override
    public BrokerController getBrokerController() {
        return brokerController;
    }

    @Override
    public MessagingProcessor getMessagingProcessor() {
        return messagingProcessor;
    }

    public ProxyServerBase setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
        return this;
    }

    public ProxyServerBase setStartAndShutdowns(List<StartAndShutdown> startAndShutdowns) {
        this.startAndShutdowns = startAndShutdowns;
        return this;
    }

    public ProxyServerBase setMessagingProcessor(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
        return this;
    }
}
