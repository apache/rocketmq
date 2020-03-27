/**
 * Copyright (C) 2010-2016 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.rocketmq.example.openmessaging.pubsub;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.OMSBuiltinKeys;
import io.openmessaging.api.OnExceptionContext;
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendCallback;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.Properties;
import org.apache.rocketmq.oms.api.PropertyKeyConst;

/**
 * MQ发送普通消息示例 Demo
 */
public class SimpleMQProducer {

    public static void main(String[] args) {

        Properties credentials = new Properties();
        credentials.setProperty(OMSBuiltinKeys.ACCESS_KEY, "accesskey");
        credentials.setProperty(OMSBuiltinKeys.SECRET_KEY, "secretkey");

        MessagingAccessPoint accessPoint = OMS.builder()
            .withCredentials(credentials)
            .driver("rocketmq")
            .endpoint("localhost:9876")
            .build();

        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, "GID-producer");
        producerProperties.setProperty(PropertyKeyConst.MsgTraceSwitch, "true");

        Producer producer = accessPoint.createProducer(producerProperties);
        producer.start();
        System.out.println("Producer Started");

        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicTest", "tagA", "mq send message test".getBytes());
            try {
                producer.sendAsync(message, new SendCallback() {
                    @Override public void onSuccess(SendResult result) {
                        System.out.println("Send result: " + result + " offset: " + result.getMessageId());
                    }

                    @Override public void onException(OnExceptionContext context) {
                        OMSRuntimeException clientException = context.getException();
                        clientException.getCause().printStackTrace();
                    }
                });
            } catch (OMSRuntimeException e) {
                e.printStackTrace();
            }
        }
    }
}
