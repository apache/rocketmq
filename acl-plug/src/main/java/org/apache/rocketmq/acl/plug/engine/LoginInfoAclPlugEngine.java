package org.apache.rocketmq.acl.plug.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.LoginInfo;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;

public abstract class LoginInfoAclPlugEngine extends AuthenticationInfoManagementAclPlugEngine {

	private Map<String, LoginInfo> loginInfoMap = new ConcurrentHashMap<>();

	@Override
	public AuthenticationInfo getAccessControl(AccessControl accessControl) {
		AuthenticationInfo authenticationInfo = super.getAccessControl(accessControl);
		LoginInfo loginInfo = new LoginInfo();
		loginInfo.setAuthenticationInfo(authenticationInfo);
		loginInfoMap.put(accessControl.getRecognition(), loginInfo);
		return authenticationInfo;
	}

	public LoginInfo getLoginInfo(AccessControl accessControl) {
		LoginInfo loginInfo = loginInfoMap.get(accessControl.getRecognition());
		if (loginInfo == null) {
			getAccessControl(accessControl);
			loginInfo = loginInfoMap.get(accessControl.getRecognition());
		}
		if (loginInfo != null) {
			loginInfo.setOperationTime(System.currentTimeMillis());
		}
		return loginInfo;
	}

	
	protected  AuthenticationInfo  getAuthenticationInfo(LoginOrRequestAccessControl accessControl , AuthenticationResult authenticationResult) {
		LoginInfo anthenticationInfo = getLoginInfo(accessControl);
		if(anthenticationInfo != null) {
			return anthenticationInfo.getAuthenticationInfo();
		}else {
			authenticationResult.setResultString("Login information does not exist");
		}
		return null;
	}
}
