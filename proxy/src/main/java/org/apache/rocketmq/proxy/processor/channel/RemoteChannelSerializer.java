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

package org.apache.rocketmq.proxy.processor.channel;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class RemoteChannelSerializer {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final String REMOTE_PROXY_IP_KEY = "remoteProxyIp";
    private static final String REMOTE_ADDRESS_KEY = "remoteAddress";
    private static final String LOCAL_ADDRESS_KEY = "localAddress";
    private static final String TYPE_KEY = "type";
    private static final String EXTEND_ATTRIBUTE_KEY = "extendAttribute";

    public static String toJson(RemoteChannel remoteChannel) {
        Map<String, Object> data = new HashMap<>();
        data.put(REMOTE_PROXY_IP_KEY, remoteChannel.getRemoteProxyIp());
        data.put(REMOTE_ADDRESS_KEY, remoteChannel.getRemoteAddress());
        data.put(LOCAL_ADDRESS_KEY, remoteChannel.getLocalAddress());
        data.put(TYPE_KEY, remoteChannel.getType());
        data.put(EXTEND_ATTRIBUTE_KEY, remoteChannel.getChannelExtendAttribute());
        return JSON.toJSONString(data);
    }

    public static RemoteChannel decodeFromJson(String jsonData) {
        if (StringUtils.isBlank(jsonData)) {
            return null;
        }
        try {
            JSONObject jsonObject = JSON.parseObject(jsonData);
            return new RemoteChannel(
                jsonObject.getString(REMOTE_PROXY_IP_KEY),
                jsonObject.getString(REMOTE_ADDRESS_KEY),
                jsonObject.getString(LOCAL_ADDRESS_KEY),
                jsonObject.getObject(TYPE_KEY, ChannelProtocolType.class),
                jsonObject.getObject(EXTEND_ATTRIBUTE_KEY, String.class)
            );
        } catch (Throwable t) {
            log.error("decode remote channel data failed. data:{}", jsonData, t);
        }
        return null;
    }
}
