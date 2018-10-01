package org.apache.rocketmq.acl.plug.engine;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.LoginInfo;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;

public interface AclPlugEngine {

	public AuthenticationInfo getAccessControl(AccessControl accessControl) ;
	
	public LoginInfo getLoginInfo(AccessControl accessControl) ;
	
	public AuthenticationResult eachCheckLoginAndAuthentication(LoginOrRequestAccessControl accessControl);
}
