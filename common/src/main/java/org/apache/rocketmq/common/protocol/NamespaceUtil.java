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
package org.apache.rocketmq.common.protocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NamespaceUtil {
    public static final char NAMESPACE_SEPARATOR = '%';
    public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
    public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

    /**
     * Parse namespace from request @see {@link RemotingCommand}, just like:
     * (Topic_XXX, MQ_INST_XX) --> MQ_INST_XX%Topic_XXX
     *
     * @param request
     * @param resource resource without namespace.
     * @return resource with namespace.
     */
    public static String withNamespace(RemotingCommand request, String resource) {
        return wrapNamespace(getNamespaceFromRequest(request), resource);
    }

    /**
     * Unpack namespace from resource, just like:
     * (1) MQ_INST_XX%Topic_XXX --> Topic_XXX
     * (2) %RETRY%MQ_INST_XX%GID_XXX --> %RETRY%GID_XXX
     *
     * @param resource
     * @return
     */
    public static String withoutNamespace(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return resource;
        }

        if (isRetryTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, RETRY_PREFIX_LENGTH);
            if (index > 0) {
                return MixAll.getRetryTopic(resource.substring(index + 1));
            }
            return resource;
        }

        if (isDLQTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, DLQ_PREFIX_LENGTH);
            if (index > 0) {
                return MixAll.getDLQTopic(resource.substring(index + 1));
            }
            return resource;
        }

        int index = resource.indexOf(NAMESPACE_SEPARATOR);
        if (index > 0) {
            return resource.substring(index + 1);
        }
        return resource;
    }

    /**
     * If resource contains the namespace, unpack namespace from resource, just like:
     * (1) (MQ_INST_XX1%Topic_XXX1, MQ_INST_XX1) --> Topic_XXX1
     * (2) (MQ_INST_XX2%Topic_XXX2, NULL) --> MQ_INST_XX2%Topic_XXX2
     * (3) (%RETRY%MQ_INST_XX1%GID_XXX1, MQ_INST_XX1) --> %RETRY%GID_XXX1
     * (4) (%RETRY%MQ_INST_XX2%GID_XXX2, MQ_INST_XX3) --> %RETRY%MQ_INST_XX2%GID_XXX2
     *
     * @param resource
     * @param namespace
     * @return
     */
    public static String withoutNamespace(String resource, String namespace) {
        if (StringUtils.isEmpty(resource) || StringUtils.isEmpty(namespace)) {
            return resource;
        }

        StringBuffer prefixBuffer = new StringBuffer();
        if (isRetryTopic(resource)) {
            prefixBuffer.append(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        } else if (isDLQTopic(resource)) {
            prefixBuffer.append(MixAll.DLQ_GROUP_TOPIC_PREFIX);
        }
        prefixBuffer.append(namespace).append(NAMESPACE_SEPARATOR);

        if (resource.startsWith(prefixBuffer.toString())) {
            return withoutNamespace(resource);
        }

        return resource;
    }

    public static String wrapNamespace(String namespace, String resource) {
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource)) {
            return resource;
        }

        if (isSystemResource(resource)) {
            return resource;
        }

        if (isAlreadyWithNamespace(resource, namespace)) {
            return resource;
        }

        StringBuffer strBuffer = new StringBuffer().append(namespace).append(NAMESPACE_SEPARATOR);

        if (isRetryTopic(resource)) {
            strBuffer.append(resource.substring(RETRY_PREFIX_LENGTH));
            return strBuffer.insert(0, MixAll.RETRY_GROUP_TOPIC_PREFIX).toString();
        }

        if (isDLQTopic(resource)) {
            strBuffer.append(resource.substring(DLQ_PREFIX_LENGTH));
            return strBuffer.insert(0, MixAll.DLQ_GROUP_TOPIC_PREFIX).toString();
        }

        return strBuffer.append(resource).toString();

    }

    public static boolean isAlreadyWithNamespace(String resource, String namespace) {
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return false;
        }

        if (isRetryTopic(resource)) {
            resource = resource.substring(RETRY_PREFIX_LENGTH);
        }

        if (isDLQTopic(resource)) {
            resource = resource.substring(DLQ_PREFIX_LENGTH);
        }

        return resource.startsWith(namespace + NAMESPACE_SEPARATOR);
    }

    public static String withNamespaceAndRetry(RemotingCommand request, String consumerGroup) {
        return wrapNamespaceAndRetry(getNamespaceFromRequest(request), consumerGroup);
    }

    public static String wrapNamespaceAndRetry(String namespace, String consumerGroup) {
        if (StringUtils.isEmpty(consumerGroup)) {
            return null;
        }

        return new StringBuffer()
            .append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            .append(wrapNamespace(namespace, consumerGroup))
            .toString();
    }

    public static String getNamespaceFromRequest(RemotingCommand request) {
        if (null == request || null == request.getExtFields()) {
            return null;
        }

        String namespace;

        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE_V2:
                namespace = request.getExtFields().get("n");
                break;
            default:
                namespace = request.getExtFields().get("namespace");
                break;
        }

        return namespace;
    }

    public static String getNamespaceFromResource(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return "";
        }

        if (isRetryTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, RETRY_PREFIX_LENGTH);
            if (index > 0) {
                return resource.substring(RETRY_PREFIX_LENGTH, index);
            }
            return "";
        }

        if (isDLQTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, DLQ_PREFIX_LENGTH);
            if (index > 0) {
                return resource.substring(DLQ_PREFIX_LENGTH, index);
            }
            return "";
        }

        int index = resource.indexOf(NAMESPACE_SEPARATOR);
        if (index > 0) {
            return resource.substring(0, index);
        }
        return "";
    }

    private static boolean isSystemResource(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        if (MixAll.isSystemTopic(resource)) {
            return true;
        }

        if (MixAll.isSysConsumerGroup(resource)) {
            return true;
        }

        return MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(resource);
    }

    public static boolean isRetryTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    public static boolean isDLQTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }
}