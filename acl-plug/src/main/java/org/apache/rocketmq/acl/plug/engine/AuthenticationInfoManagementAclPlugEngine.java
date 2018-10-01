package org.apache.rocketmq.acl.plug.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.acl.plug.AccessContralAnalysis;
import org.apache.rocketmq.acl.plug.Authentication;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;
import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategy;
import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategyFactory;

public abstract class AuthenticationInfoManagementAclPlugEngine implements AclPlugEngine {

	
	private Map<String/**account **/ , Map<String/**netaddress**/ , AuthenticationInfo>> accessControlMap = new HashMap<>();
	
	private AuthenticationInfo  authenticationInfo;
	
	private NetaddressStrategyFactory netaddressStrategyFactory = new NetaddressStrategyFactory();
	
	private AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
	
	private Authentication authentication = new Authentication();
	
	public void setAccessControl(AccessControl accessControl) {
		try {
			NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
			Map<String , AuthenticationInfo> accessControlAddressMap = accessControlMap.get(accessControl.getAccount());
			if(accessControlAddressMap == null ) {
				accessControlAddressMap = new HashMap<>();
				accessControlMap.put(accessControl.getAccount(), accessControlAddressMap);
			}
			accessControlAddressMap.put(accessControl.getNetaddress(), new AuthenticationInfo(accessContralAnalysis.analysis(accessControl),accessControl ,netaddressStrategy));
		}catch(Exception e) {
			// TODO Exception
		}
	}
	
	public void setAccessControlList(List<AccessControl> AccessControlList) {
		for(AccessControl accessControl : AccessControlList) {
			setAccessControl(accessControl);
		}
	}
	
	
	public void setNetaddressAccessControl(AccessControl accessControl) {
		authenticationInfo = new AuthenticationInfo(accessContralAnalysis.analysis(accessControl) , accessControl, netaddressStrategyFactory.getNetaddressStrategy(accessControl));
	}
	
	public AuthenticationInfo getAccessControl(AccessControl accessControl) {
		AuthenticationInfo existing = null;
		if( accessControl.getAccount() == null && authenticationInfo != null) {
			existing = authenticationInfo.getNetaddressStrategy().match(accessControl)?authenticationInfo:null;
		}else {
			Map<String, AuthenticationInfo> accessControlAddressMap = accessControlMap.get(accessControl.getAccount());
			if(accessControlAddressMap != null ) {
				existing = accessControlAddressMap.get(accessControl.getNetaddress());
				if(existing.getAccessControl().getPassword().equals(accessControl.getPassword())) {
					if( existing.getNetaddressStrategy().match(accessControl)) {
						return existing;
					}
				}
				existing = null;
			}
		}
		return existing;
	}
	
	@Override
	public AuthenticationResult eachCheckLoginAndAuthentication(LoginOrRequestAccessControl accessControl) {
		AuthenticationResult authenticationResult = new AuthenticationResult();
		AuthenticationInfo authenticationInfo = getAuthenticationInfo(accessControl , authenticationResult);
		if(authenticationInfo != null) {			
			boolean boo = authentication.authentication(authenticationInfo, accessControl,authenticationResult);
			authenticationResult.setSucceed( boo );
		}
		return authenticationResult;
	}
	
	protected abstract AuthenticationInfo  getAuthenticationInfo(LoginOrRequestAccessControl accessControl , AuthenticationResult authenticationResult);
}
