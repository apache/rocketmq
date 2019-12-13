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
package org.apache.rocketmq.ons.api.impl.authority.exception;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.ons.api.Constants;

public class OnsSessionCredentials extends SessionCredentials {

    public static final String SignatureMethod = "SignatureMethod";

    private String onsChannel;

    public static final String KEY_FILE = System.getProperty("rocketmq.client.keyFile",
        System.getProperty("user.home") + File.separator + "onskey");

    private String signatureMethod;

    public OnsSessionCredentials() {
        String keyContent = null;
        try {
            keyContent = MixAll.file2String(KEY_FILE);
        } catch (IOException ignore) {
        }
        if (keyContent != null) {
            Properties prop = MixAll.string2Properties(keyContent);
            if (prop != null) {
                this.updateContent(prop);
            }
        }
    }

    @Override
    public void updateContent(Properties properties) {
        super.updateContent(properties);
        {
            String value = properties.getProperty(Constants.ONS_CHANNEL_KEY);
            if (value != null) {
                this.onsChannel = value;
            }
        }

    }

    public static String getSignatureMethod() {
        return SignatureMethod;
    }

    public String getOnsChannel() {
        return onsChannel;
    }

    public void setOnsChannel(String onsChannel) {
        this.onsChannel = onsChannel;
    }

    public void setSignatureMethod(String signatureMethod) {
        this.signatureMethod = signatureMethod;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof OnsSessionCredentials))
            return false;
        if (!super.equals(o))
            return false;
        OnsSessionCredentials that = (OnsSessionCredentials) o;
        return Objects.equals(onsChannel, that.onsChannel) &&
            Objects.equals(signatureMethod, that.signatureMethod);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), onsChannel, signatureMethod);
    }
}
