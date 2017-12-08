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
package org.apache.rocketmq.logappender.log4j2;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logappender.common.ProducerInstance;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.SerializedLayout;
import org.apache.rocketmq.client.producer.MQProducer;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Log4j2 Appender Component
 */
@Plugin(name = "RocketMQ",
    category = Node.CATEGORY,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public class RocketmqLog4j2Appender extends AbstractAppender {

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

    /**
     * Appended message tag define
     */
    private String tag;

    /**
     * Whitch topic to send log messages
     */
    private String topic;

    protected RocketmqLog4j2Appender(String name, Filter filter, Layout<? extends Serializable> layout,
        boolean ignoreExceptions, String nameServerAddress, String producerGroup,
        String topic, String tag) {
        super(name, filter, layout, ignoreExceptions);
        this.producer = producer;
        this.topic = topic;
        this.tag = tag;
        this.nameServerAddress = nameServerAddress;
        this.producerGroup = producerGroup;
        try {
            this.producer = ProducerInstance.getProducerInstance().getInstance(this.nameServerAddress, this.producerGroup);
        } catch (Exception e) {
            ErrorHandler handler = this.getHandler();
            if (handler != null) {
                handler.error("Starting RocketmqLog4j2Appender [" + this.getName()
                    + "] nameServerAddress:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
            }
        }
    }

    /**
     * Info,error,warn,callback method implementation
     */
    public void append(LogEvent event) {
        if (null == producer) {
            return;
        }
        byte[] data = this.getLayout().toByteArray(event);
        try {
            Message msg = new Message(topic, tag, data);
            msg.getProperties().put(ProducerInstance.APPENDER_TYPE, ProducerInstance.LOG4J2_APPENDER);

            //Send message and do not wait for the ack from the message broker.
            producer.sendOneway(msg);
        } catch (Exception e) {
            ErrorHandler handler = this.getHandler();
            if (handler != null) {
                String msg = new String(data);
                handler.error("Could not send message in RocketmqLog4j2Appender [" + this.getName() + "].Message is : " + msg, e);
            }

        }
    }

    /**
     * When system exit,this method will be called to close resources
     */
    public boolean stop(long timeout, TimeUnit timeUnit) {
        this.setStopping();
        try {
            ProducerInstance.getProducerInstance().removeAndClose(this.nameServerAddress, this.producerGroup);
        } catch (Exception e) {
            ErrorHandler handler = this.getHandler();
            if (handler != null) {
                handler.error("Closeing RocketmqLog4j2Appender [" + this.getName()
                    + "] nameServerAddress:" + nameServerAddress + " group:" + producerGroup + " " + e.getMessage());
            }
        }

        boolean stopped = super.stop(timeout, timeUnit, false);
        this.setStopped();
        return stopped;
    }

    /**
     * Log4j2 builder creator
     */
    @PluginBuilderFactory
    public static RocketmqLog4j2Appender.Builder newBuilder() {
        return new RocketmqLog4j2Appender.Builder();
    }

    /**
     * Log4j2 xml builder define
     */
    public static class Builder implements org.apache.logging.log4j.core.util.Builder<RocketmqLog4j2Appender> {

        @PluginBuilderAttribute
        @Required(message = "A name for the RocketmqLog4j2Appender must be specified")
        private String name;

        @PluginElement("Layout")
        private Layout<? extends Serializable> layout;

        @PluginElement("Filter")
        private Filter filter;

        @PluginBuilderAttribute
        private boolean ignoreExceptions;

        @PluginBuilderAttribute
        private String tag;

        @PluginBuilderAttribute
        private String nameServerAddress;

        @PluginBuilderAttribute
        private String producerGroup;

        @PluginBuilderAttribute
        @Required(message = "A topic name must be specified")
        private String topic;

        private Builder() {
            this.layout = SerializedLayout.createLayout();
            this.ignoreExceptions = true;
        }

        public RocketmqLog4j2Appender.Builder setName(String name) {
            this.name = name;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setLayout(Layout<? extends Serializable> layout) {
            this.layout = layout;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setFilter(Filter filter) {
            this.filter = filter;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setIgnoreExceptions(boolean ignoreExceptions) {
            this.ignoreExceptions = ignoreExceptions;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setTag(final String tag) {
            this.tag = tag;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setNameServerAddress(String nameServerAddress) {
            this.nameServerAddress = nameServerAddress;
            return this;
        }

        public RocketmqLog4j2Appender.Builder setProducerGroup(String producerGroup) {
            this.producerGroup = producerGroup;
            return this;
        }

        public RocketmqLog4j2Appender build() {
            return new RocketmqLog4j2Appender(name, filter, layout, ignoreExceptions,
                nameServerAddress, producerGroup, topic, tag);
        }
    }
}
