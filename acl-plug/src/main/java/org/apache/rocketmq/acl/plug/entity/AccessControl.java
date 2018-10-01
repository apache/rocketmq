package org.apache.rocketmq.acl.plug.entity;

public class AccessControl {

	private String account;
	
	private String password;
	
	private String netaddress;

	private String recognition;
	
	public AccessControl() {
	}
	
	
	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getNetaddress() {
		return netaddress;
	}

	public void setNetaddress(String netaddress) {
		this.netaddress = netaddress;
	}

	public String getRecognition() {
		return recognition;
	}

	public void setRecognition(String recognition) {
		this.recognition = recognition;
	}

	@Override
	public String toString() {
		return "AccessControl [account=" + account + ", password=" + password + ", netaddress=" + netaddress
				+ ", recognition=" + recognition + "]";
	}
	
	
	
}
