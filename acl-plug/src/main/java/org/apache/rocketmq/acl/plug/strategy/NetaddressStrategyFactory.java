package org.apache.rocketmq.acl.plug.strategy;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plug.AclUtils;
import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class NetaddressStrategyFactory {

	
	
	public  NetaddressStrategy getNetaddressStrategy(AccessControl accessControl ) {
		String netaddress = accessControl.getNetaddress();
		if(StringUtils.isBlank(netaddress) || "*".equals(netaddress) ) {//*
			return NullNetaddressStrategy.NULL_NET_ADDRESS_STRATEGY;
		}
		if(netaddress.endsWith("}")) {//1.1.1.{1,2,3,4,5}
			String[] strArray  = StringUtils.split(netaddress);
			String four = strArray[3];
			if(!four.startsWith("{")) {
				
			}
			return new MultipleNetaddressStrategy(AclUtils.getAddreeStrArray(netaddress, four));
		}else if(AclUtils.isColon(netaddress)) {//1.1.1.1,1.2.3.4.5
			return new MultipleNetaddressStrategy( StringUtils.split(","));
		}else if(AclUtils.isAsterisk(netaddress) || AclUtils.isMinus(netaddress)) {//1.2.*.*　，　1.1.1.1-5　，1.1.1-5.*
			return new RangeNetaddressStrategy(netaddress);
		}
		return new OneNetaddressStrategy(netaddress);
		
	}
}
