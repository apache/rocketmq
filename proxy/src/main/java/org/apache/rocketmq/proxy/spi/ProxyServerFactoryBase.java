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

import org.apache.rocketmq.acl.AccessValidator;

import java.util.List;

public abstract class ProxyServerFactoryBase implements ProxyServerFactory {

    protected List<AccessValidator> validators;
    protected ProxyServerInitializer initializer;

    @Override
    public ProxyServerFactory withAccessValidators(List<AccessValidator> accessValidators) {
        this.validators = accessValidators;
        return this;
    }

    @Override
    public ProxyServerFactory withInitializer(ProxyServerInitializer proxyServerInitializer) {
        this.initializer = proxyServerInitializer;
        return this;
    }

    @Override
    public final ProxyServerBase get() {
        ProxyServerBase serverBase = build();
        serverBase.setBrokerController(this.initializer.getBrokerController());
        serverBase.setStartAndShutdowns(this.initializer.getStartAndShutdowns());
        serverBase.setMessagingProcessor(this.initializer.getMessagingProcessor());
        return serverBase;
    }

    protected abstract ProxyServerBase build();
}
