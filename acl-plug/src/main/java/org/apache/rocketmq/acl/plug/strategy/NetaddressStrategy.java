package org.apache.rocketmq.acl.plug.strategy;

import org.apache.rocketmq.acl.plug.entity.AccessControl;

public interface NetaddressStrategy {

	
	public boolean match(AccessControl accessControl);
}
