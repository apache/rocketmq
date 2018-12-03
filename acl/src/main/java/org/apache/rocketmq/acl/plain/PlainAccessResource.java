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
package org.apache.rocketmq.acl.plain;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.common.MixAll;

public class PlainAccessResource implements AccessResource {
    //identify the user
    private String accessKey;

    private String signature;
    //the content to calculate the content
    private byte[] content;

    private String secretToken;

    private Map<String, Byte> resourcePermMap = new HashMap<>();

    private String remoteAddr;

    private String recognition;

    private int requestCode;


    @Deprecated
    private String topic;

    public PlainAccessResource() {
    }

    public void addResourceAndPerm(String resource, byte perm) {
        if (resource == null) {
            return;
        }
        resourcePermMap.put(resource, perm);
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRecognition() {
        return recognition;
    }

    public void setRecognition(String recognition) {
        this.recognition = recognition;
    }

    public int getRequestCode() {
        return requestCode;
    }

    public void setRequestCode(int requestCode) {
        this.requestCode = requestCode;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSecretToken() {
        return secretToken;
    }

    public void setSecretToken(String secretToken) {
        this.secretToken = secretToken;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }


    public static boolean isRetryTopic(String topic) {
        return (null != topic && topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX));
    }

    public static String getRetryTopic(String group) {
        if (group == null) {
            return null;
        }
        return MixAll.getRetryTopic(group);
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}
