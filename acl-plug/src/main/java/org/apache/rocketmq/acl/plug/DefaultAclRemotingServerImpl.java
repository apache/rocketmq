package org.apache.rocketmq.acl.plug;

import org.apache.rocketmq.acl.plug.engine.AclPlugEngine;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;

public class DefaultAclRemotingServerImpl implements AclRemotingServer {

	private AclPlugEngine aclPlugEngine;
	
	public DefaultAclRemotingServerImpl(AclPlugEngine aclPlugEngine ) {
		this.aclPlugEngine = aclPlugEngine;
	}
	
	@Override
	public AuthenticationInfo login() {
		
		return null;
	}

	@Override
	public AuthenticationInfo eachCheck(LoginOrRequestAccessControl accessControl) {
		aclPlugEngine.eachCheckLoginAndAuthentication(accessControl);
		return null;
	}

}
