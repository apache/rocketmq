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

public class MessageReply extends Message {

    private String consumerGroup;
    private String consumerResult;
    private long consumerTimeStamp;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerResult() {
        return consumerResult;
    }

    public void setConsumerResult(String consumerResult) {
        this.consumerResult = consumerResult;
    }

    public long getConsumerTimeStamp() {
        return consumerTimeStamp;
    }

    public void setConsumerTimeStamp(long consumerTimeStamp) {
        this.consumerTimeStamp = consumerTimeStamp;
    }
}
