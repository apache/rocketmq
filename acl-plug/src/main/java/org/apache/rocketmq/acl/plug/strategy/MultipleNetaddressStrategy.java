package org.apache.rocketmq.acl.plug.strategy;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class MultipleNetaddressStrategy extends AbstractNetaddressStrategy {

	private final Set<String> multipleSet = new HashSet<>();
	
	public MultipleNetaddressStrategy(String[] strArray) {
		for(String netaddress : strArray) {
			verify(netaddress, 4);
			multipleSet.add(netaddress);
		}
	}
	
	
	@Override
	public boolean match(AccessControl accessControl) {
		return multipleSet.contains(accessControl.getNetaddress());
	}

}
