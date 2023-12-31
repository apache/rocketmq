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
package org.apache.rocketmq.auth.authorization.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

public class Resource implements Comparable<Resource> {

    private ResourceType resourceType;

    private String resourceName;

    private ResourcePattern resourcePattern;

    public static Resource ofCluster(String clusterName) {
        return of(ResourceType.CLUSTER, clusterName, ResourcePattern.LITERAL);
    }

    public static Resource ofTopic(String topicName) {
        return of(ResourceType.TOPIC, topicName, ResourcePattern.LITERAL);
    }

    public static Resource ofGroup(String groupName) {
        if (NamespaceUtil.isRetryTopic(groupName)) {
            groupName = NamespaceUtil.withOutRetryAndDLQ(groupName);
        }
        return of(ResourceType.GROUP, groupName, ResourcePattern.LITERAL);
    }

    public static Resource of(ResourceType resourceType, String resourceName, ResourcePattern resourcePattern) {
        Resource resource = new Resource();
        resource.resourceType = resourceType;
        resource.resourceName = resourceName;
        resource.resourcePattern = resourcePattern;
        return resource;
    }

    public static List<Resource> of(List<String> resourceKeys) {
        if (CollectionUtils.isEmpty(resourceKeys)) {
            return null;
        }
        return resourceKeys.stream().map(Resource::of).collect(Collectors.toList());
    }

    public static Resource of(String resourceKey) {
        if (StringUtils.equals(resourceKey, CommonConstants.ASTERISK)) {
            return of(ResourceType.ANY, null, ResourcePattern.ANY);
        }
        String type = StringUtils.substringBefore(resourceKey, CommonConstants.COLON);
        ResourceType resourceType = ResourceType.getByName(type);
        if (resourceType == null) {
            return null;
        }
        String resourceName = StringUtils.substringAfter(resourceKey, CommonConstants.COLON);
        ResourcePattern resourcePattern = ResourcePattern.LITERAL;
        if (StringUtils.equals(resourceName, CommonConstants.ASTERISK)) {
            resourceName = null;
            resourcePattern = ResourcePattern.ANY;
        } else if (StringUtils.endsWith(resourceName, CommonConstants.ASTERISK)) {
            resourceName = StringUtils.substringBefore(resourceName, CommonConstants.ASTERISK);
            resourcePattern = ResourcePattern.PREFIXED;
        }
        return of(resourceType, resourceName, resourcePattern);
    }

    public String toResourceKey() {
        if (resourceType == ResourceType.ANY) {
            return CommonConstants.ASTERISK;
        }
        switch (resourcePattern) {
            case ANY:
                return resourceType.getName() + CommonConstants.COLON + CommonConstants.ASTERISK;
            case LITERAL:
                return resourceType.getName() + CommonConstants.COLON + resourceName;
            case PREFIXED:
                return resourceType.getName() + CommonConstants.COLON + resourceName + CommonConstants.ASTERISK;
            default:
                return null;
        }
    }

    public boolean isMatch(Resource resource) {
        if (this.resourceType == ResourceType.ANY) {
            return true;
        }
        if (this.resourceType != resource.resourceType) {
            return false;
        }
        switch (resourcePattern) {
            case ANY:
                return true;
            case LITERAL:
                return StringUtils.equals(resource.resourceName, this.resourceName);
            case PREFIXED:
                return StringUtils.startsWith(resource.resourceName, this.resourceName);
            default:
                return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Resource resource = (Resource) o;
        return resourceType == resource.resourceType
            && Objects.equals(resourceName, resource.resourceName)
            && resourcePattern == resource.resourcePattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, resourceName, resourcePattern);
    }

    @Override
    public int compareTo(Resource o) {
        if (this.resourcePattern == o.resourcePattern) {
            if (this.resourcePattern == ResourcePattern.LITERAL
                || this.resourcePattern == ResourcePattern.ANY) {
                return 0;
            }
            return Integer.compare(this.resourceName.length(), o.resourceName.length());
        } else {
            if (this.resourcePattern == ResourcePattern.LITERAL) {
                return 1;
            }
            if (o.resourcePattern == ResourcePattern.LITERAL) {
                return -1;
            }
            if (this.resourcePattern == ResourcePattern.PREFIXED) {
                return 1;
            }
            if (o.resourcePattern == ResourcePattern.PREFIXED) {
                return -1;
            }
        }

        return 0;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public void setResourcePattern(ResourcePattern resourcePattern) {
        this.resourcePattern = resourcePattern;
    }
}
