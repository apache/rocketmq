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

package org.apache.rocketmq.example.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSampleProducer {

    private static Logger log = LoggerFactory.getLogger(MqttSampleProducer.class);

    public static void main(String[] args) throws InterruptedException {
        String topic = "mqtt-sample";
        String messageContent = "hello mqtt";
        int qos = 0;
        String broker = "tcp://127.0.0.1:1883";
        String clientId = "JavaSampleProducer";

        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setKeepAliveInterval(6000);
            log.info("Connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            log.info("Connected");
            log.info("Publishing message: " + messageContent);
            MqttMessage message = new MqttMessage(messageContent.getBytes());
            message.setQos(qos);
            message.setRetained(true);
            sampleClient.publish(topic, message);
            log.info("Message published");
            /*sampleClient.disconnect();
            log.info("Disconnected");*/
            Thread.sleep(10000000);
        } catch (MqttException me) {
            log.error("reason " + me.getReasonCode());
            log.error("msg " + me.getMessage());
            log.error("loc " + me.getLocalizedMessage());
            log.error("cause " + me.getCause());
            log.error("excep " + me);
            me.printStackTrace();
            System.exit(1);
        }
    }
}
