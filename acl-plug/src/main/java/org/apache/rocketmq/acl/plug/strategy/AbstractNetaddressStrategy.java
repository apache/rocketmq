package org.apache.rocketmq.acl.plug.strategy;

import org.apache.rocketmq.acl.plug.AclUtils;

public abstract class AbstractNetaddressStrategy implements NetaddressStrategy {

	public void verify(String netaddress , int index) {
		AclUtils.isScope(netaddress, index);
	}

}
