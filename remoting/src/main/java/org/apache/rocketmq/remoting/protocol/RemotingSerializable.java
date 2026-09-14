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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class RemotingSerializable {
    private final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static byte[] encode(final Object obj) {
        if (obj == null) {
            return null;
        }
        return JSON.toJSONBytes(obj, CHARSET_UTF8);
    }

    public static String toJson(final Object obj, boolean prettyFormat) {
        if (prettyFormat) {
            return JSON.toJSONString(obj, JSONWriter.Feature.PrettyFormat);
        }
        return JSON.toJSONString(obj);
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        if (data == null) {
            return null;
        }
        return JSON.parseObject(data, classOfT);
    }

    public static <T> List<T> decodeList(final byte[] data, Class<T> classOfT) {
        if (data == null) {
            return null;
        }
        return JSON.parseArray(data, 0, data.length, CHARSET_UTF8, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    public byte[] encode() {
        return JSON.toJSONBytes(this, CHARSET_UTF8);
    }

    /**
     * Allow call-site to apply specific features according to their requirements.
     *
     * @param features Features to apply
     * @return serialized data.
     */
    public byte[] encode(JSONWriter.Feature... features) {
        return JSON.toJSONBytes(this, CHARSET_UTF8, features);
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}