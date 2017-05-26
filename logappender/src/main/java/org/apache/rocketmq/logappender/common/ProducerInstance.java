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
package org.apache.rocketmq.logappender.common;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Common Producer component
 */
public class ProducerInstance {

    public static final String APPENDER_TYPE = "APPENDER_TYPE";

    public static final String LOG4J_APPENDER = "LOG4J_APPENDER";

    public static final String LOG4J2_APPENDER = "LOG4J2_APPENDER";

    public static final String LOGBACK_APPENDER = "LOGBACK_APPENDER";

    public static final String DEFAULT_GROUP = "rocketmq_appender";

    private static ConcurrentHashMap<String, MQProducer> producerMap = new ConcurrentHashMap<String, MQProducer>();

    private static String genKey(String nameServerAddress, String group) {
        return nameServerAddress + "_" + group;
    }


    public static MQProducer getInstance(String nameServerAddress, String group) throws MQClientException {
        if (group == null) {
            group = DEFAULT_GROUP;
        }

        String genKey = genKey(nameServerAddress, group);
        MQProducer p = producerMap.get(genKey);
        if (p != null) {
            return p;
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(group);
        defaultMQProducer.setNamesrvAddr(nameServerAddress);
        MQProducer beforeProducer = null;
        //cas put producer
        beforeProducer = producerMap.putIfAbsent(genKey, defaultMQProducer);
        if (beforeProducer != null) {
            return beforeProducer;
        }
        defaultMQProducer.start();
        return defaultMQProducer;
    }


    public static void removeAndClose(String nameServerAddress, String group) {
        if (group == null) {
            group = DEFAULT_GROUP;
        }
        String genKey = genKey(nameServerAddress, group);
        MQProducer producer = producerMap.remove(genKey);

        if (producer != null) {
            producer.shutdown();
        }
    }

    public static void closeAll() {
        Set<Map.Entry<String, MQProducer>> entries = producerMap.entrySet();
        for (Map.Entry<String, MQProducer> entry : entries) {
            producerMap.remove(entry.getKey());
            entry.getValue().shutdown();
        }
    }

}
