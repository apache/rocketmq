package org.apache.rocketmq.acl.plug.entity;

public class LoginInfo {

	
	private String recognition;

	private long loginTime = System.currentTimeMillis();
	
	private long operationTime = loginTime;
	
	private AuthenticationInfo  authenticationInfo;
	
	
	
	public AuthenticationInfo getAuthenticationInfo() {
		return authenticationInfo;
	}

	public void setAuthenticationInfo(AuthenticationInfo authenticationInfo) {
		this.authenticationInfo = authenticationInfo;
	}

	public String getRecognition() {
		return recognition;
	}

	public void setRecognition(String recognition) {
		this.recognition = recognition;
	}

	public long getLoginTime() {
		return loginTime;
	}

	public void setLoginTime(long loginTime) {
		this.loginTime = loginTime;
	}

	public long getOperationTime() {
		return operationTime;
	}

	public void setOperationTime(long operationTime) {
		this.operationTime = operationTime;
	}

	@Override
	public String toString() {
		return "LoginInfo [recognition=" + recognition + ", loginTime=" + loginTime + ", operationTime=" + operationTime
				+ ", authenticationInfo=" + authenticationInfo + "]";
	}
	
	
}
