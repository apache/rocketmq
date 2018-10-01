package org.apache.rocketmq.acl.plug;

import org.apache.commons.lang3.StringUtils;

public class AclUtils {

	
	public static String[] getAddreeStrArray(String netaddress ,String four ) {
		String[] fourStrArray = StringUtils.split(four.substring(1, four.length()-1) , ",");
		String address = netaddress.substring(0, netaddress.indexOf("{")  );
		String[] addreeStrArray = new String[ fourStrArray.length ];
		for(int i = 0 ; i < fourStrArray.length ; i++) {
			addreeStrArray[i] = address+fourStrArray[i];
		}
		return addreeStrArray;
	}
	
	public static boolean isScope(String num, int index) {
		String[] strArray = StringUtils.split(num , ".");
		if(strArray.length != 4) {
			return false;
		}
		return isScope(strArray, index);

	}
	
	public static boolean isScope(String[] num, int index) {
		if (num.length <= index) {

		}
		for (int i = 0; i < index; i++) {
			if( !isScope(num[i])) {
				return false;
			}
		}
		return true;

	}

	public static boolean isScope(String num) {
		return isScope(Integer.valueOf(num.trim()));
	}

	public static boolean isScope(int num) {
		return num >= 0 && num <= 255;
	}

	public static boolean isAsterisk(String asterisk) {
		return asterisk.indexOf('*') > -1;
	}

	public static boolean isColon(String colon) {
		return colon.indexOf(',') > -1;
	}

	public static boolean isMinus(String minus) {
		return minus.indexOf('-') > -1;

	}
}
