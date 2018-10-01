package org.apache.rocketmq.acl.plug.entity;

/**
 * @author Administrator
 *
 */
public class LoginOrRequestAccessControl extends AccessControl {

	
	private int code;
	
	private String topic;

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("LoginOrRequestAccessControl [code=").append(code).append(", topic=").append(topic).append("]");
		return builder.toString();
	}
	
	
	
}
