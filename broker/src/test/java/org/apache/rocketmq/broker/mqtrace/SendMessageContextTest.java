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
package org.apache.rocketmq.broker.mqtrace;

import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendMessageContextTest {
    private Map<String, String> fields2Type = new HashMap<>();

    private String producerGroup;
    private String topic;
    private String msgId;
    private String originMsgId;
    private Integer queueId;
    private Long queueOffset;
    private String brokerAddr;
    private String bornHost;
    private int bodyLength;
    private int code;
    private String errorMsg;
    private String msgProps;
    private Object mqTraceContext;
    private Properties extProps;
    private String brokerRegionId;
    private String msgUniqueKey;
    private long bornTimeStamp;
    private MessageType msgType = MessageType.Trans_msg_Commit;
    private boolean isSuccess = false;

    private String commercialOwner;
    private BrokerStatsManager.StatsType commercialSendStats;
    private int commercialSendSize;
    private int commercialSendTimes;
    private String namespace;
    @Before
    public void init() {
        fields2Type.put("producerGroup", String.class.toString());
        fields2Type.put("topic", String.class.toString());
        fields2Type.put("msgId", String.class.toString());
        fields2Type.put("originMsgId", String.class.toString());
        fields2Type.put("queueId", Integer.class.toString());
        fields2Type.put("queueOffset", Long.class.toString());
        fields2Type.put("brokerAddr", String.class.toString());
        fields2Type.put("bornHost", String.class.toString());
        fields2Type.put("bodyLength", "int");
        fields2Type.put("code", "int");
        fields2Type.put("errorMsg", String.class.toString());
        fields2Type.put("msgProps", String.class.toString());
        fields2Type.put("mqTraceContext", Object.class.toString());
        fields2Type.put("extProps", Properties.class.toString());
        fields2Type.put("brokerRegionId", String.class.toString());
        fields2Type.put("msgUniqueKey", String.class.toString());
        fields2Type.put("bornTimeStamp", "long");
        fields2Type.put("msgType", MessageType.class.toString());
        fields2Type.put("isSuccess", "boolean");
        fields2Type.put("commercialOwner", String.class.toString());
        fields2Type.put("commercialSendStats", BrokerStatsManager.StatsType.class.toString());
        fields2Type.put("commercialSendSize", "int");
        fields2Type.put("commercialSendTimes", "int");
        fields2Type.put("namespace", String.class.toString());
    }

    @Test
    public void testContextFormat() {
        SendMessageContext sendMessageContext = new SendMessageContext();
        Class cls = sendMessageContext.getClass();
        Field[] fields = cls.getDeclaredFields();
        for(Field field : fields) {
            String name = field.getName();
            String clsName = field.getType().toString();
            String expectClsName = fields2Type.get(name);
            assertThat(clsName.equals(expectClsName)).isTrue();
        }
    }

}