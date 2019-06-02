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
package io.openmessaging.rocketmq.domain;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.consumer.MessageReceipt;
import io.openmessaging.extension.ExtensionHeader;
import io.openmessaging.message.Header;
import io.openmessaging.message.Message;
import java.util.Arrays;

public class BytesMessageImpl implements Message {

    private Header sysHeaders;
    private ExtensionHeader extensionHeader;
    private MessageReceipt messageReceipt;
    private KeyValue userProperties;
    private byte[] data;

    public BytesMessageImpl() {
        this.sysHeaders = new MessageHeader();
        this.userProperties = OMS.newKeyValue();
        this.extensionHeader = new MessageExtensionHeader();
        this.messageReceipt = new DefaultMessageReceipt();
    }

    @Override
    public Header header() {
        return sysHeaders;
    }

    @Override
    public ExtensionHeader extensionHeader() {
        return extensionHeader;
    }

    @Override
    public KeyValue properties() {
        return userProperties;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    @Override
    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public MessageReceipt getMessageReceipt() {
        return messageReceipt;
    }

    @Override public String toString() {
        return "BytesMessageImpl{" +
            "sysHeaders=" + sysHeaders +
            ", extensionHeader=" + extensionHeader +
            ", messageReceipt=" + messageReceipt +
            ", userProperties=" + userProperties +
            ", data=" + Arrays.toString(data) +
            '}';
    }
}
