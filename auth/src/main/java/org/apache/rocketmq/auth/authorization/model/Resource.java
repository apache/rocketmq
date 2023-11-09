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

    public static List<Resource> parseResources(List<String> resourceKeys) {
        if (CollectionUtils.isEmpty(resourceKeys)) {
            return null;
        }
        return resourceKeys.stream().map(Resource::parseResource).collect(Collectors.toList());
    }

    public String toResourceKey() {
        if (resourceType == ResourceType.ANY) {
            return CommonConstants.ASTERISK;
        }
        switch (resourcePattern) {
            case ANY:
                return resourceType.getCode() + CommonConstants.COLON + CommonConstants.ASTERISK;
            case LITERAL:
                return resourceType.getCode() + CommonConstants.COLON + resourceName;
            case PREFIXED:
                return resourceType.getCode() + CommonConstants.COLON + resourceName + CommonConstants.ASTERISK;
            default:
                return null;
        }
    }

    public static Resource parseResource(String resourceKey) {
        if (StringUtils.equals(resourceKey, CommonConstants.ASTERISK)) {
            return of(ResourceType.ANY, null, ResourcePattern.ANY);
        }
        String type = StringUtils.substringBefore(resourceKey, CommonConstants.COLON);
        ResourceType resourceType = ResourceType.getByCode(type);
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

    public boolean isMatch(Resource resource) {
        if (this.resourceType != resource.resourceType) {
            return false;
        }
        if (StringUtils.equals(this.resourceName, "*")) {
            return true;
        }
        switch (resourcePattern) {
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
            if (this.resourceName.length() > o.resourceName.length()) {
                return 1;
            } else if (this.resourceName.length() == o.resourceName.length()) {
                return 0;
            } else {
                return -1;
            }
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
