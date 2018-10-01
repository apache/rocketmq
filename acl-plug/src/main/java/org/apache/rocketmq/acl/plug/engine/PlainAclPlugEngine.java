package org.apache.rocketmq.acl.plug.engine;

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControlTransport;
import org.yaml.snakeyaml.Yaml;

public class PlainAclPlugEngine extends LoginInfoAclPlugEngine {

	public PlainAclPlugEngine() {
		init();
	}
	
	void init() {
		Yaml ymal = new Yaml();
		BorkerAccessControlTransport transport = ymal.loadAs(PlainAclPlugEngine.class.getClassLoader().getResourceAsStream( "transport.yml"), BorkerAccessControlTransport.class);
		super.setNetaddressAccessControl(transport.getOnlyNetAddress());
		for(AccessControl accessControl : transport.getList()) {
			super.setAccessControl(accessControl);
		}
	}
	
}
