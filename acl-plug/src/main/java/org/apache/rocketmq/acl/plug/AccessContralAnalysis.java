package org.apache.rocketmq.acl.plug;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rocketmq.acl.plug.annotation.RequestCode;
import org.apache.rocketmq.acl.plug.entity.AccessControl;

public class AccessContralAnalysis {

	private Map<Class<?>, Map<Integer, Field>> classTocodeAndMentod = new HashMap<>();

	public Map<Integer , Boolean> analysis(AccessControl accessControl) {
		Class<? extends AccessControl> clazz = accessControl.getClass();
		Map<Integer, Field> codeAndField = classTocodeAndMentod.get(clazz);
		if (codeAndField == null) {
			codeAndField = new HashMap<>();
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				RequestCode requestCode = field.getAnnotation(RequestCode.class);
				if (requestCode != null) {
					int code = requestCode.code();
					if (codeAndField.containsKey(code)) {

					} else {
						field.setAccessible(true);
						codeAndField.put(code, field);
					}
				}

			}
			classTocodeAndMentod.put(clazz, codeAndField);
		}
		Iterator<Entry<Integer, Field>> it = codeAndField.entrySet().iterator();
		Map<Integer, Boolean> authority = new HashMap<>();
		try {
			while (it.hasNext()) {
				Entry<Integer, Field> e = it.next();
				authority.put(e.getKey(), (Boolean)e.getValue().get(accessControl));
			}
		} catch (IllegalArgumentException | IllegalAccessException e1) {
			e1.printStackTrace();
		}
		return authority;
	}

}
