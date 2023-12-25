package org.apache.rocketmq.common.action;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.rocketmq.common.resource.ResourceType;

@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQAction {

    int value();

    ResourceType resource() default ResourceType.UNKNOWN;

    Action[] action();
}
