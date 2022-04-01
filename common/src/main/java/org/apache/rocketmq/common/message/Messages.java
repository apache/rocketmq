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
package org.apache.rocketmq.common.message;

import java.util.Collection;
import java.util.Map;

public class Messages {

    public static Message synchronizedMessage(Message message) {
        return new ThreadSafeMessage(message);
    }

    private static class ThreadSafeMessage extends Message {

        private final Message delegate;

        private ThreadSafeMessage(Message delegate) {
            this.delegate = delegate;
        }

        public void setKeys(String keys) {
            synchronized (this.delegate) {
                this.delegate.setKeys(keys);
            }
        }

        void putProperty(final String name, final String value) {
            synchronized (this.delegate) {
                this.delegate.putProperty(name, value);
            }
        }

        void clearProperty(final String name) {
            synchronized (this.delegate) {
                this.delegate.clearProperty(name);
            }
        }

        public void putUserProperty(final String name, final String value) {
            synchronized (this.delegate) {
                this.delegate.putUserProperty(name, value);
            }
        }

        public String getUserProperty(final String name) {
            synchronized (this.delegate) {
                return this.delegate.getUserProperty(name);
            }
        }

        public String getProperty(final String name) {
            synchronized (this.delegate) {
                return this.delegate.getProperty(name);
            }
        }

        public String getTopic() {
            synchronized (this.delegate.getTopic()) {
                return this.delegate.getTopic();
            }
        }

        public void setTopic(String topic) {
            synchronized (this.delegate) {
                this.delegate.setTopic(topic);
            }
        }

        public String getTags() {
            synchronized (this.delegate) {
                return this.delegate.getTags();
            }
        }

        public void setTags(String tags) {
            synchronized (this.delegate) {
                this.delegate.setTags(tags);
            }
        }

        public String getKeys() {
            synchronized (this.delegate) {
                return this.delegate.getKeys();
            }
        }

        public void setKeys(Collection<String> keys) {
            synchronized (this.delegate) {
                this.delegate.setKeys(keys);
            }
        }

        public int getDelayTimeLevel() {
            synchronized (this.delegate) {
                return this.delegate.getDelayTimeLevel();
            }
        }

        public void setDelayTimeLevel(int level) {
            synchronized (this.delegate) {
                this.delegate.setDelayTimeLevel(level);
            }
        }

        public boolean isWaitStoreMsgOK() {
            synchronized (this.delegate) {
                return this.delegate.isWaitStoreMsgOK();
            }
        }

        public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
            synchronized (this.delegate) {
                this.delegate.setWaitStoreMsgOK(waitStoreMsgOK);
            }
        }

        public void setInstanceId(String instanceId) {
            synchronized (this.delegate) {
                this.delegate.setInstanceId(instanceId);
            }
        }

        public int getFlag() {
            synchronized (this.delegate) {
                return this.delegate.getFlag();
            }
        }

        public void setFlag(int flag) {
            synchronized (this.delegate) {
                this.delegate.setFlag(flag);
            }
        }

        public byte[] getBody() {
            synchronized (this.delegate) {
                return this.delegate.getBody();
            }
        }

        public void setBody(byte[] body) {
            synchronized (this.delegate) {
                this.delegate.setBody(body);
            }
        }

        public Map<String, String> getProperties() {
            synchronized (this.delegate) {
                return this.delegate.getProperties();
            }
        }

        void setProperties(Map<String, String> properties) {
            synchronized (this.delegate) {
                this.delegate.setProperties(properties);
            }
        }

        public String getBuyerId() {
            synchronized (this.delegate) {
                return this.delegate.getBuyerId();
            }
        }

        public void setBuyerId(String buyerId) {
            synchronized (this.delegate) {
                this.delegate.setBuyerId(buyerId);
            }
        }

        public String getTransactionId() {
            synchronized (this.delegate) {
                return this.delegate.getTransactionId();
            }
        }

        public void setTransactionId(String transactionId) {
            synchronized (this.delegate) {
                this.delegate.setTransactionId(transactionId);
            }
        }

    }
}
