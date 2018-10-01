package org.apache.rocketmq.acl.plug;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;

public class Authentication {

	public boolean authentication(AuthenticationInfo authenticationInfo, LoginOrRequestAccessControl loginOrRequestAccessControl,AuthenticationResult authenticationResult) {
		int code = loginOrRequestAccessControl.getCode();
		if (authenticationInfo.getAuthority().get(code)) {
			AccessControl accessControl = authenticationInfo.getAccessControl();
			if( !(accessControl instanceof BorkerAccessControl)) {
				return true;
			}
			BorkerAccessControl borker = (BorkerAccessControl) authenticationInfo.getAccessControl();
			String topicName = loginOrRequestAccessControl.getTopic();
			if (code == 10 || code == 310 || code == 320) {
				if (borker.getPermitSendTopic().contains(topicName)) {
					return true;
				}
				if (borker.getNoPermitSendTopic().contains(topicName)) {
					authenticationResult.setResultString(String.format("noPermitSendTopic include %s", topicName));
					return false;
				}
				return true;
			} else if (code == 11) {
				if (borker.getPermitPullTopic().contains(topicName)) {
					return true;
				}
				if (borker.getNoPermitPullTopic().contains(topicName)) {
					authenticationResult.setResultString(String.format("noPermitPullTopic include %s", topicName));
					return false;
				}
				return true;
			}
			return true;
		}
		return false;
	}
}
