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
package org.apache.rocketmq.logappender.logback;

import ch.qos.logback.classic.net.LoggingEventPreSerializationTransformer;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.spi.PreSerializationTransformer;
import ch.qos.logback.core.status.ErrorStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logappender.common.ProducerInstance;
import org.apache.rocketmq.client.producer.MQProducer;

/**
 * Logback Appender Component
 */
public class RocketmqLogbackAppender extends AppenderBase<ILoggingEvent> {

    /**
     * Message tag define
     */
    private String tag;

    /**
     * Whitch topic to send log messages
     */
    private String topic;

    /**
     * RocketMQ nameserver address
     */
    private String nameServerAddress;

    /**
     * Log producer group
     */
    private String producerGroup;

    /**
     * Log producer send instance
     */
    private MQProducer producer;

    private Layout layout;

    private PreSerializationTransformer<ILoggingEvent> pst = new LoggingEventPreSerializationTransformer();

    /**
     * Info,error,warn,callback method implementation
     */
    @Override
    protected void append(ILoggingEvent event) {
        if (!isStarted()) {
            return;
        }
        String logStr = this.layout.doLayout(event);
        try {
            Message msg = new Message(topic, tag, logStr.getBytes());
            msg.getProperties().put(ProducerInstance.APPENDER_TYPE, ProducerInstance.LOGBACK_APPENDER);

            //Send message and do not wait for the ack from the message broker.
            producer.sendOneway(msg);
        } catch (Exception e) {
            addError("Could not send message in RocketmqLogbackAppender [" + name + "]. Message is : " + logStr, e);
        }
    }

    /**
     * Options are activated and become effective only after calling this method.
     */
    public void start() {
        int errors = 0;

        if (this.layout == null) {
            addStatus(new ErrorStatus("No layout set for the RocketmqLogbackAppender named \"" + name + "\".", this));
            errors++;
        }

        if (errors > 0 || !checkEntryConditions()) {
            return;
        }
        try {
            producer = ProducerInstance.getProducerInstance().getInstance(nameServerAddress, producerGroup);
        } catch (Exception e) {
            addError("Starting RocketmqLogbackAppender [" + this.getName()
                + "] nameServerAddress:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
        }
        if (producer != null) {
            super.start();
        }
    }

    /**
     * When system exit,this method will be called to close resources
     */
    public synchronized void stop() {
        // The synchronized modifier avoids concurrent append and close operations
        if (!this.started) {
            return;
        }

        this.started = false;

        try {
            ProducerInstance.getProducerInstance().removeAndClose(this.nameServerAddress, this.producerGroup);
        } catch (Exception e) {
            addError("Closeing RocketmqLogbackAppender [" + this.getName()
                + "] nameServerAddress:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
        }

        // Help garbage collection
        producer = null;
    }

    protected boolean checkEntryConditions() {
        String fail = null;

        if (this.topic == null) {
            fail = "No topic";
        }

        if (fail != null) {
            addError(fail + " for RocketmqLogbackAppender named [" + name + "].");
            return false;
        } else {
            return true;
        }
    }

    public Layout getLayout() {
        return this.layout;
    }

    /**
     * Set the pattern layout to format the log.
     */
    public void setLayout(Layout layout) {
        this.layout = layout;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
