package org.apache.rocketmq.acl.plug.entity;

import java.util.Map;

import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategy;

public class AuthenticationInfo {

	private AccessControl accessControl;
	
	private NetaddressStrategy netaddressStrategy;

	private Map<Integer, Boolean> authority;
	
	public AuthenticationInfo(Map<Integer, Boolean> authority , AccessControl accessControl, NetaddressStrategy netaddressStrategy) {
		super();
		this.authority = authority;
		this.accessControl = accessControl;
		this.netaddressStrategy = netaddressStrategy;
	}

	public AccessControl getAccessControl() {
		return accessControl;
	}

	public void setAccessControl(AccessControl accessControl) {
		this.accessControl = accessControl;
	}

	public NetaddressStrategy getNetaddressStrategy() {
		return netaddressStrategy;
	}

	public void setNetaddressStrategy(NetaddressStrategy netaddressStrategy) {
		this.netaddressStrategy = netaddressStrategy;
	}

	
	
	public Map<Integer, Boolean> getAuthority() {
		return authority;
	}

	public void setAuthority(Map<Integer, Boolean> authority) {
		this.authority = authority;
	}

	@Override
	public String toString() {
		return "AuthenticationInfo [accessControl=" + accessControl + ", netaddressStrategy=" + netaddressStrategy
				+ ", authority=" + authority + "]";
	}
	
	
	
}
