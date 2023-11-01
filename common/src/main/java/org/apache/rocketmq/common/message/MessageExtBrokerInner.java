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

import java.nio.ByteBuffer;

import org.apache.rocketmq.common.TopicFilterType;

import static org.apache.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static org.apache.rocketmq.common.message.MessageDecoder.PROPERTY_SEPARATOR;

public class MessageExtBrokerInner extends MessageExt {
    private static final long serialVersionUID = 7256001576878700634L;
    private String propertiesString;
    private long tagsCode;

    private ByteBuffer encodedBuff;

    private volatile boolean encodeCompleted;

    private MessageVersion version = MessageVersion.MESSAGE_VERSION_V1;

    public ByteBuffer getEncodedBuff() {
        return encodedBuff;
    }

    public void setEncodedBuff(ByteBuffer encodedBuff) {
        this.encodedBuff = encodedBuff;
    }

    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.length() == 0) { return 0; }

        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags) {
        return tagsString2tagsCode(null, tags);
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }


    public void deleteProperty(String name) {
        super.clearProperty(name);
        if (propertiesString != null) {
            int idx0 = 0;
            int idx1;
            int idx2;
            idx1 = propertiesString.indexOf(name, idx0);
            if (idx1 != -1) {
                // cropping may be required
                StringBuilder stringBuilder = new StringBuilder(propertiesString.length());
                while (true) {
                    int startIdx = idx0;
                    while (true) {
                        idx1 = propertiesString.indexOf(name, startIdx);
                        if (idx1 == -1) {
                            break;
                        }
                        startIdx = idx1 + name.length();
                        if (idx1 == 0 || propertiesString.charAt(idx1 - 1) == PROPERTY_SEPARATOR) {
                            if (propertiesString.length() > idx1 + name.length()
                                && propertiesString.charAt(idx1 + name.length()) == NAME_VALUE_SEPARATOR) {
                                break;
                            }
                        }
                    }
                    if (idx1 == -1) {
                        // there are no characters that need to be skipped. Append all remaining characters.
                        stringBuilder.append(propertiesString, idx0, propertiesString.length());
                        break;
                    }
                    // there are characters that need to be cropped
                    stringBuilder.append(propertiesString, idx0, idx1);
                    // move idx2 to the end of the cropped character
                    idx2 = propertiesString.indexOf(PROPERTY_SEPARATOR, idx1 + name.length() + 1);
                    // all subsequent characters will be cropped
                    if (idx2 == -1) {
                        break;
                    }
                    idx0 = idx2 + 1;
                }
                this.setPropertiesString(stringBuilder.toString());
            }
        }
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public MessageVersion getVersion() {
        return version;
    }

    public void setVersion(MessageVersion version) {
        this.version = version;
    }

    public void removeWaitStorePropertyString() {
        if (this.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = this.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            this.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            this.setPropertiesString(MessageDecoder.messageProperties2String(this.getProperties()));
        }
    }

    public boolean isEncodeCompleted() {
        return encodeCompleted;
    }

    public void setEncodeCompleted(boolean encodeCompleted) {
        this.encodeCompleted = encodeCompleted;
    }
}
