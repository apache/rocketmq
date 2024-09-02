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

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.MapDeserializer;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * workaround https://github.com/alibaba/fastjson/issues/3730
 */
public class GenericMapSuperclassDeserializer implements ObjectDeserializer {
    public static final GenericMapSuperclassDeserializer INSTANCE = new GenericMapSuperclassDeserializer();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
        Class<?> clz = (Class<?>) type;
        Type genericSuperclass = clz.getGenericSuperclass();
        Map map;
        try {
            map = (Map) clz.newInstance();
        } catch (Exception e) {
            throw new JSONException("unsupported type " + type, e);
        }
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
        Type keyType = parameterizedType.getActualTypeArguments()[0];
        Type valueType = parameterizedType.getActualTypeArguments()[1];
        if (String.class == keyType) {
            return (T) MapDeserializer.parseMap(parser, (Map<String, Object>) map, valueType, fieldName);
        } else {
            return (T) MapDeserializer.parseMap(parser, map, keyType, valueType, fieldName);
        }
    }

    @Override
    public int getFastMatchToken() {
        return JSONToken.LBRACE;
    }
}
