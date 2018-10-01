package org.apache.rocketmq.acl.plug.entity;

import java.util.List;

public class BorkerAccessControlTransport {

	private BorkerAccessControl onlyNetAddress;
	
	private List<BorkerAccessControl> list;

	

	public BorkerAccessControlTransport() {
		super();
	}

	public BorkerAccessControl getOnlyNetAddress() {
		return onlyNetAddress;
	}

	public void setOnlyNetAddress(BorkerAccessControl onlyNetAddress) {
		this.onlyNetAddress = onlyNetAddress;
	}

	public List<BorkerAccessControl> getList() {
		return list;
	}

	public void setList(List<BorkerAccessControl> list) {
		this.list = list;
	}

	@Override
	public String toString() {
		return "BorkerAccessControlTransport [onlyNetAddress=" + onlyNetAddress + ", list=" + list + "]";
	}
	
	
	
}
