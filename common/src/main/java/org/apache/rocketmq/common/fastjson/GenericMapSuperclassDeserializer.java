/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.fastjson;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.reader.ObjectReader;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * workaround https://github.com/alibaba/fastjson/issues/3730
 */
public class GenericMapSuperclassDeserializer implements ObjectReader<Object> {
    public static final GenericMapSuperclassDeserializer INSTANCE = new GenericMapSuperclassDeserializer();
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Object readObject(JSONReader reader, Type type, Object fieldName, long features) {
        Class<?> clz = (Class<?>) type;
        Type genericSuperclass = clz.getGenericSuperclass();
        Map map;
        try {
            map = (Map) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new JSONException("unsupport type " + type, e);
        }
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
        Type keyType = parameterizedType.getActualTypeArguments()[0];
        Type valueType = parameterizedType.getActualTypeArguments()[1];

        if (!reader.nextIfObjectStart()) {
            throw new JSONException(reader.info("expect '{', but " + reader.current()));
        }

        while (!reader.nextIfObjectEnd()) {
            Object key;
            if (keyType == String.class) {
                key = reader.readFieldName();
            } else {
                key = reader.getContext().getProvider().getObjectReader(keyType).readObject(reader, keyType, fieldName, features);
                reader.nextIfMatch(':');
            }

            Object value = reader.getContext().getProvider().getObjectReader(valueType).readObject(reader, valueType, fieldName, features);
            map.put(key, value);
            reader.nextIfComma();
        }
        return map;
    }
}
