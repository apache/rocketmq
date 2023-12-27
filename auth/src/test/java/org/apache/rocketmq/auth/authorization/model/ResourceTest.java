package org.apache.rocketmq.auth.authorization.model;

import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.junit.Assert;
import org.junit.Test;

public class ResourceTest {

    @Test
    public void parseResource() {
        Resource resource = Resource.parseResource("*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.ANY);
        Assert.assertNull(resource.getResourceName());
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.ANY);

        resource = Resource.parseResource("Topic:*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertNull(resource.getResourceName());
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.ANY);

        resource = Resource.parseResource("Topic:test-*");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertEquals(resource.getResourceName(), "test-");
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.PREFIXED);

        resource = Resource.parseResource("Topic:test-1");
        Assert.assertEquals(resource.getResourceType(), ResourceType.TOPIC);
        Assert.assertEquals(resource.getResourceName(), "test-1");
        Assert.assertEquals(resource.getResourcePattern(), ResourcePattern.LITERAL);
    }

    @Test
    public void isMatch() {

    }
}