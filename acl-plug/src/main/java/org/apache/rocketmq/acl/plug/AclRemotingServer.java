package org.apache.rocketmq.acl.plug;

import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;

public interface AclRemotingServer {

	
	public AuthenticationInfo login();
	
	
	public AuthenticationInfo eachCheck(LoginOrRequestAccessControl accessControl);
	
}
