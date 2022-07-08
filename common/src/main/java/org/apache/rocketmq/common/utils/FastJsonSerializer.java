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
package org.apache.rocketmq.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.config.FastJsonConfig;
import org.apache.commons.lang3.SerializationException;

/**
 * The object serializer based on fastJson
 */
public class FastJsonSerializer implements Serializer {
    private FastJsonConfig fastJsonConfig = new FastJsonConfig();

    public FastJsonConfig getFastJsonConfig() {
        return this.fastJsonConfig;
    }

    public void setFastJsonConfig(FastJsonConfig fastJsonConfig) {
        this.fastJsonConfig = fastJsonConfig;
    }

    @Override
    public <T> byte[] serialize(T t) throws SerializationException {
        if (t == null) {
            return new byte[0];
        } else {
            try {
                return JSON.toJSONBytes(this.fastJsonConfig.getCharset(), t, this.fastJsonConfig.getSerializeConfig(), this.fastJsonConfig.getSerializeFilters(), this.fastJsonConfig.getDateFormat(), JSON.DEFAULT_GENERATE_FEATURE, this.fastJsonConfig.getSerializerFeatures());
            } catch (Exception var3) {
                throw new SerializationException("Could not serialize: " + var3.getMessage(), var3);
            }
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException {
        if (bytes != null && bytes.length != 0) {
            try {
                return JSON.parseObject(bytes, this.fastJsonConfig.getCharset(), type, this.fastJsonConfig.getParserConfig(), this.fastJsonConfig.getParseProcess(), JSON.DEFAULT_PARSER_FEATURE, this.fastJsonConfig.getFeatures());
            } catch (Exception var3) {
                throw new SerializationException("Could not deserialize: " + var3.getMessage(), var3);
            }
        } else {
            return null;
        }
    }
}
