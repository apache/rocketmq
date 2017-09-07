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
package org.apache.rocketmq.logappender.log4j;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logappender.common.ProducerInstance;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.rocketmq.client.producer.MQProducer;

/**
 * Log4j Appender Component
 */
public class RocketmqLog4jAppender extends AppenderSkeleton {

    /**
     * Appended message tag define
     */
    private String tag;

    /**
     * Whitch topic to send log messages
     */
    private String topic;

    private boolean locationInfo;

    /**
     * Log producer send instance
     */
    private MQProducer producer;

    /**
     * RocketMQ nameserver address
     */
    private String nameServerAddress;

    /**
     * Log producer group
     */
    private String producerGroup;

    public RocketmqLog4jAppender() {
    }

    public void activateOptions() {
        LogLog.debug("Getting initial context.");
        if (!checkEntryConditions()) {
            return;
        }
        try {
            producer = ProducerInstance.getProducerInstance().getInstance(nameServerAddress, producerGroup);
        } catch (Exception e) {
            LogLog.error("activateOptions nameserver:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
        }
    }

    /**
     * Info,error,warn,callback method implementation
     */
    public void append(LoggingEvent event) {
        if (null == producer) {
            return;
        }
        if (locationInfo) {
            event.getLocationInformation();
        }
        byte[] data = this.layout.format(event).getBytes();
        try {
            Message msg = new Message(topic, tag, data);
            msg.getProperties().put(ProducerInstance.APPENDER_TYPE, ProducerInstance.LOG4J_APPENDER);

            //Send message and do not wait for the ack from the message broker.
            producer.sendOneway(msg);
        } catch (Exception e) {
            String msg = new String(data);
            errorHandler.error("Could not send message in RocketmqLog4jAppender [" + name + "].Message is :" + msg, e,
                ErrorCode.GENERIC_FAILURE);
        }
    }

    protected boolean checkEntryConditions() {
        String fail = null;

        if (this.topic == null) {
            fail = "No topic";
        } else if (this.tag == null) {
            fail = "No tag";
        }

        if (fail != null) {
            errorHandler.error(fail + " for RocketmqLog4jAppender named [" + name + "].");
            return false;
        } else {
            return true;
        }
    }

    /**
     * When system exit,this method will be called to close resources
     */
    public synchronized void close() {
        // The synchronized modifier avoids concurrent append and close operations

        if (this.closed)
            return;

        LogLog.debug("Closing RocketmqLog4jAppender [" + name + "].");
        this.closed = true;

        try {
            ProducerInstance.getProducerInstance().removeAndClose(this.nameServerAddress, this.producerGroup);
        } catch (Exception e) {
            LogLog.error("Closing RocketmqLog4jAppender [" + name + "] nameServerAddress:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
        }
        // Help garbage collection
        producer = null;
    }

    public boolean requiresLayout() {
        return true;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    /**
     * Returns value of the <b>LocationInfo</b> property which
     * determines whether location (stack) info is sent to the remote
     * subscriber.
     */
    public boolean isLocationInfo() {
        return locationInfo;
    }

    /**
     * If true, the information sent to the remote subscriber will
     * include caller's location information. By default no location
     * information is sent to the subscriber.
     */
    public void setLocationInfo(boolean locationInfo) {
        this.locationInfo = locationInfo;
    }

    /**
     * Returns the message producer,Only valid after
     * activateOptions() method has been invoked.
     */
    protected MQProducer getProducer() {
        return producer;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
