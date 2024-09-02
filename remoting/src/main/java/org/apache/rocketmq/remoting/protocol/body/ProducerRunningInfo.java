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
package org.apache.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
 
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.common.message.MessageQueue;
 
 
 
public class ProducerRunningInfo extends RemotingSerializable {
    public static final String PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    public static final String PROP_COMPRESS_LEVEL = "PROP_COMPRESS_LEVEL";
    public static final String PROP_COMPRESS_TYPE = "PROP_COMPRESS_TYPE";
    public static final String PROPS_SERVICE_STATE = "PROP_SERVICE_STATE";
 
    private Properties properties = new Properties();
 
    private Set<String> publishTopicList = new HashSet<>();
 
    ConcurrentHashMap<String /* Topic */, List<MessageQueue>> producerTopicsMessageOueue = new ConcurrentHashMap<>();
 
    ConcurrentHashMap<MessageQueue, ConcurrentHashMap<String, Long>> mqOffsetData = new ConcurrentHashMap<>(); 
 
    ConcurrentHashMap<MessageQueue, Long> mqEarliestMsgStoreTime = new ConcurrentHashMap<>();
 
 
 
    public Properties getProperties() {
        return properties;
    }
 
    public Set<String> getPublishTopicList() {
        return publishTopicList;
    }
 
    public ConcurrentHashMap<String /* Topic */, List<MessageQueue>> getProducerTopicsMessageOueue() {
        return producerTopicsMessageOueue;
    }
 
    public ConcurrentHashMap<MessageQueue, ConcurrentHashMap<String, Long>> getMqOffsetData() {
        return mqOffsetData;
    }
 
    public ConcurrentHashMap<MessageQueue, Long> getMqEarliestMsgStoreTime() {
        return mqEarliestMsgStoreTime;
    }
 
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
 
    public void setPublishTopicList(Set<String> publishTopicList) {
        this.publishTopicList = publishTopicList;
    }
 
    public void setProducerTopicsMessageOueue(ConcurrentHashMap<String /* Topic */, List<MessageQueue>> producerTopicsMessageOueue) {
        this.producerTopicsMessageOueue = producerTopicsMessageOueue;
    }
 
    public void setMqOffsetData(ConcurrentHashMap<MessageQueue, ConcurrentHashMap<String, Long>> mqOffsetData) {
        this.mqOffsetData = mqOffsetData;
    }
 
    public void setMqEarliestMsgStoreTime(ConcurrentHashMap<MessageQueue, Long> mqEarliestMsgStoreTime) {
        this.mqEarliestMsgStoreTime = mqEarliestMsgStoreTime;
    }
}
