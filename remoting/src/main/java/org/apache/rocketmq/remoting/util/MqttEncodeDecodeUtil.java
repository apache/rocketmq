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

package org.apache.rocketmq.remoting.util;

import com.google.gson.Gson;
import java.nio.charset.Charset;

public class MqttEncodeDecodeUtil {
    private static final Gson GSON = new Gson();

    public static byte[] encode(Object object) {
        final String json = GSON.toJson(object);
        if (json != null) {
            return json.getBytes(Charset.forName("UTF-8"));
        }
        return null;
    }

    public static <T> Object decode(byte[] body, Class<T> classOfT) {
        final String json = new String(body, Charset.forName("UTF-8"));
        return GSON.fromJson(json, classOfT);
    }
}
