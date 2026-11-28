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

import com.google.common.base.Strings;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.utils.MessageUtils;

public class MessageExtBrokerInner extends MessageExt {
    private static final long serialVersionUID = 7256001576878700634L;
    private String propertiesString;
    /** Pre-encoded UTF-8 bytes for {@link #propertiesString}. Either set directly via
     *  {@link #setPropertiesData} on the broker write hot path (skipping the
     *  String round-trip), or lazily computed from {@link #propertiesString}. */
    private transient byte[] propertiesData;
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
        if (Strings.isNullOrEmpty(tags)) { return 0; }

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
        this.propertiesData = null;
    }

    public byte[] getPropertiesData() {
        // Defensive copy: callers must not be able to mutate the cached encoded bytes,
        // which are reused as-is by the encoder. The encode hot path should call
        // {@link #getEffectivePropertiesData()} (package-private) to avoid this copy.
        return propertiesData == null ? null : propertiesData.clone();
    }

    public void setPropertiesData(byte[] propertiesData) {
        this.propertiesData = propertiesData;
    }

    /** Encoder-side accessor: returns cached {@link #propertiesData} when set,
     *  otherwise lazily encodes {@link #propertiesString} and caches the result.
     *  Returns null if neither is set.
     *  <p>Package-private and intended for the broker encode hot path only. The returned
     *  array is the internal buffer (no defensive copy) and must not be mutated by callers. */
    byte[] getEffectivePropertiesData() {
        if (propertiesData != null) {
            return propertiesData;
        }
        if (propertiesString != null) {
            propertiesData = propertiesString.getBytes(MessageDecoder.CHARSET_UTF8);
            return propertiesData;
        }
        return null;
    }


    public void deleteProperty(String name) {
        super.clearProperty(name);
        if (propertiesString != null) {
            this.propertiesString = MessageUtils.deleteProperty(propertiesString, name);
        }
        if (propertiesData != null) {
            this.propertiesData = MessageDecoder.messageProperties2Bytes(getProperties());
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

    public boolean needDispatchLMQ() {
        return StringUtils.isNoneBlank(getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
            && MixAll.topicAllowsLMQ(getTopic());
    }
}
