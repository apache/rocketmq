package org.apache.rocketmq.acl.plug;

import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;

public class EmptyImplementationAclRemotingServer implements AclRemotingServer {

	@Override
	public AuthenticationInfo login() {
		
		return null;
	}

	@Override
	public AuthenticationInfo eachCheck() {
		// TODO Auto-generated method stub
		return null;
	}

}
