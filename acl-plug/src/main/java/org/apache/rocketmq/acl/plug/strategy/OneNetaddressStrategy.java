package org.apache.rocketmq.acl.plug.strategy;

import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class OneNetaddressStrategy extends AbstractNetaddressStrategy {

	
	private String netaddress;
	
	public OneNetaddressStrategy(String netaddress) {
		this.netaddress = netaddress;
	}
	
	@Override
	public boolean match(AccessControl accessControl) {
		return netaddress.equals(accessControl.getNetaddress());
	}

}
