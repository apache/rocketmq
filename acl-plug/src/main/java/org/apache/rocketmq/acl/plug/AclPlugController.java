package org.apache.rocketmq.acl.plug;

import org.apache.rocketmq.acl.plug.engine.AclPlugEngine;
import org.apache.rocketmq.acl.plug.engine.PlainAclPlugEngine;
import org.apache.rocketmq.acl.plug.entity.ControllerParametersEntity;

public class AclPlugController {

	
	private ControllerParametersEntity controllerParametersEntity;
	
	private AclPlugEngine aclPlugEngine;
	
	private AclRemotingServer aclRemotingServer;
	
	public AclPlugController(ControllerParametersEntity controllerParametersEntity){
		this.controllerParametersEntity = controllerParametersEntity;
		aclPlugEngine = new PlainAclPlugEngine();
		aclRemotingServer = new DefaultAclRemotingServerImpl(aclPlugEngine);
	}
	
	public AclRemotingServer getAclRemotingServer() {
		return this.aclRemotingServer;
	}
	
	
	public boolean isStartSucceed() {
		return true;
	}
}
