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
package org.apache.rocketmq.client.trace.hook.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

enum RocketMqObservationDocumentation implements ObservationDocumentation {

    MESSAGE_IN {
        @Override
        public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
            return DefaultRocketMqReceiverObservationConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return LowCardinalityTags.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HighCardinalityTags.values();
        }

    },

    MESSAGE_OUT {
        @Override
        public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
            return DefaultRocketMqSenderObservationConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return LowCardinalityTags.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HighCardinalityTags.values();
        }
    };

    // TODO: Do we want to set all tags from TraceConstants?
    /**
     * Low cardinality tags.
     */
    public enum LowCardinalityTags implements KeyName {

        /**
         * A string identifying the messaging system.
         */
        MESSAGING_SYSTEM {
            @Override
            public String asString() {
                return "messaging.system";
            }
        },

        /**
         * A string identifying the kind of messaging operation.
         */
        MESSAGING_OPERATION {
            @Override
            public String asString() {
                return "messaging.operation";
            }
        },

        /**
         * A string identifying the protocol.
         */
        NET_PROTOCOL_NAME {
            @Override
            public String asString() {
                return "net.protocol.name";
            }
        },

        /**
         * A string identifying the protocol version.
         */
        NET_PROTOCOL_VERSION {
            @Override
            public String asString() {
                return "net.protocol.version";
            }
        },

        /**
         * Type of message.
         */
        MESSAGING_ROCKETMQ_MESSAGE_TYPE {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.type";
            }
        },
    }

    // TODO: Do we want to set all tags from TraceConstants?
    /**
     * High cardinality tags.
     */
    public enum HighCardinalityTags implements KeyName {

        /**
         * The message destination name.
         */
        MESSAGING_DESTINATION_NAME {
            @Override
            public String asString() {
                return "messaging.destination.name";
            }
        },

        // TODO: How to get this?
        /**
         * The message source name.
         */
        MESSAGING_SOURCE_NAME {
            @Override
            public String asString() {
                return "messaging.source.name";
            }
        },

        /**
         * Message payload size in bytes.
         */
        MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES {
            @Override
            public String asString() {
                return "messaging.message.payload_size_bytes";
            }
        },

        /**
         * Peer port.
         */
        NET_SOCK_PEER_PORT {
            @Override
            public String asString() {
                return "net.sock.peer.port";
            }
        },

        /**
         * Peer address.
         */
        NET_SOCK_PEER_ADDR {
            @Override
            public String asString() {
                return "net.sock.peer.addr";
            }
        },

        /**
         * Namespace of RocketMQ resources, resources in different namespaces are individual.
         */
        MESSAGING_ROCKETMQ_NAMESPACE {
            @Override
            public String asString() {
                return "messaging.rocketmq.namespace";
            }
        },

        /**
         * Name of the RocketMQ producer/consumer group that is handling the message. The client type is identified by the SpanKind.
         */
        MESSAGING_ROCKETMQ_CLIENT_GROUP {
            @Override
            public String asString() {
                return "messaging.rocketmq.client_group";
            }
        },

        /**
         * The timestamp in milliseconds that the delay message is expected to be delivered to consumer.
         */
        MESSAGING_ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.delivery_timestamp";
            }
        },

        // TODO: How to get this?
        /**
         * The delay time level for delay message, which determines the message delay time.
         */
        MESSAGING_ROCKETMQ_MESSAGE_DELAY_TIME_LEVEL {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.delay_time_level";
            }
        },

        /**
         * It is essential for FIFO message. Messages that belong to the same message group are always processed one by one within the same consumer group.
         */
        MESSAGING_ROCKETMQ_MESSAGE_GROUP {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.group";
            }
        },

        /**
         * The secondary classifier of message besides topic.
         */
        MESSAGING_ROCKETMQ_MESSAGE_TAG {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.tag";
            }
        },

        /**
         * The secondary classifier of message besides topic.
         */
        MESSAGING_ROCKETMQ_MESSAGE_KEYS {
            @Override
            public String asString() {
                return "messaging.rocketmq.message.keys";
            }
        },

        // TODO: How to get this?
        /**
         * Model of message consumption. This only applies to consumer spans.
         */
        MESSAGING_ROCKETMQ_CONSUMPTION_MODEL {
            @Override
            public String asString() {
                return "messaging.rocketmq.consumption_model";
            }
        }
    }
}
