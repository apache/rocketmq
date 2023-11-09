package org.apache.rocketmq.acl2.model;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl2.enums.ResourceType;
import org.apache.rocketmq.acl2.enums.ResourcePattern;

public class Resource {

    private ResourceType resourceType;

    private String resourceName;

    private ResourcePattern resourcePattern;

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
