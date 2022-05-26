package org.apache.rocketmq.common.route;

import java.util.List;

public class Network {

	private String tag;
	private List<String> subnet;
	
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public List<String> getSubnet() {
		return subnet;
	}
	public void setSubnet(List<String> subnet) {
		this.subnet = subnet;
	}
}
