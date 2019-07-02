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

package org.apache.rocketmq.mqtt.persistence.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;

public interface PersistService {
    /**
     * Init Persist Service
     * @param processor MQTT messages processor
     */
    void init(DefaultMqttMessageProcessor processor);

    boolean isClient2SubsriptionPersisted(Client client);

    boolean addOrUpdateClient2Susbscription(Client client, Subscription subscription);

    boolean deleteClient(Client client);

    Map<String, Set<Client>> getSnodeAddress2Clients(String topic);

    boolean clientUnsubscribe(Client client, List<String> topics);

    Subscription getSubscriptionByClientId(String clientId);

    Client getClientByClientId(String clientId);
}
