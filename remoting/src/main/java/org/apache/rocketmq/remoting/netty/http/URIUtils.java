package org.apache.rocketmq.remoting.netty.http;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class URIUtils {

	
	
	private static final Map<Integer , String> CODE_URI_MAP = new HashMap<Integer, String>();
	
	private static final Map<String,Integer >  URI_CODE_MAP = new HashMap<String, Integer>();
	
	static {
		
		try {
			Class<?> clazz = Class.forName("org.apache.rocketmq.common.protocol.RequestCode");
			Field[] fields = clazz.getDeclaredFields();
			for(Field field : fields) {
				if ( field.getType() == int.class) {
					int code = (Integer)field.get(null);
					String[] nameSpilt = field.getName().split("_");
					StringBuffer sb = new StringBuffer(  ).append('/').append( nameSpilt[0].toLowerCase());
					for(int i= 1 ; i < nameSpilt.length ; i++ ) {
						sb.append( nameSpilt[i].substring(0	, 1)).append(nameSpilt[i].substring(1).toLowerCase());
						
					}
					String uri  = sb.toString();
					CODE_URI_MAP.put( code ,  uri );
					URI_CODE_MAP.put(uri, code);
				}
			}
		} catch (ClassNotFoundException e ) {
			
		} catch (IllegalArgumentException e) {
			
		} catch (IllegalAccessException e) {
			
		}
	}
	
	public static int getCode(String uri) {
		return URI_CODE_MAP.get(uri);
	}
	
	public static String getURI(Integer code) {
		return CODE_URI_MAP.get(code);
	}
}
