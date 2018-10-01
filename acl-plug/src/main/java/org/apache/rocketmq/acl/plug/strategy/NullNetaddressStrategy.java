package org.apache.rocketmq.acl.plug.strategy;

import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class NullNetaddressStrategy implements NetaddressStrategy {

	public static final NullNetaddressStrategy NULL_NET_ADDRESS_STRATEGY =  new NullNetaddressStrategy();
	
	
	@Override
	public boolean match(AccessControl accessControl) {
		return true;
	}

}
