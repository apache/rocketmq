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

package org.apache.rocketmq.common.producer;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * handle to recall a message, only support delay message for now
 * v1 pattern like this:
 * version topic brokerName timestamp messageId
 * use Base64 to encode it
 */
public class RecallMessageHandle {
    private static final String SEPARATOR = " ";
    private static final String VERSION_1 = "v1";

    public static class HandleV1 extends RecallMessageHandle {
        private String version;
        private String topic;
        private String brokerName;
        private String timestampStr;
        private String messageId; // id of unique key

        public HandleV1(String topic, String brokerName, String timestamp, String messageId) {
            this.version = VERSION_1;
            this.topic = topic;
            this.brokerName = brokerName;
            this.timestampStr = timestamp;
            this.messageId = messageId;
        }

        // no param check
        public static String buildHandle(String topic, String brokerName, String timestampStr, String messageId) {
            String rawString = String.join(SEPARATOR, VERSION_1, topic, brokerName, timestampStr, messageId);
            return Base64.getUrlEncoder().encodeToString(rawString.getBytes(UTF_8));
        }

        public String getTopic() {
            return topic;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public String getTimestampStr() {
            return timestampStr;
        }

        public String getMessageId() {
            return messageId;
        }

        public String getVersion() {
            return version;
        }
    }

    public static RecallMessageHandle decodeHandle(String handle) throws DecoderException {
        if (StringUtils.isEmpty(handle)) {
            throw new DecoderException("recall handle is invalid");
        }
        String rawString;
        try {
            rawString =
                new String(Base64.getUrlDecoder().decode(handle.getBytes(UTF_8)), UTF_8);
        } catch (IllegalArgumentException e) {
            throw new DecoderException("recall handle is invalid");
        }
        String[] items = rawString.split(SEPARATOR);
        if (!VERSION_1.equals(items[0]) || items.length < 5) {
            throw new DecoderException("recall handle is invalid");
        }
        return new HandleV1(items[1], items[2], items[3], items[4]);
    }
}
