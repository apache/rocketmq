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
package org.apache.rocketmq.common.flowcontrol;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class FlowControlConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String flowControlFileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private static final String DEFAULT_FLOW_CONTROL_FILE = "conf/flow_control.yml";

    private String flowControlFileName = System.getProperty("rocketmq.flow.control.file", DEFAULT_FLOW_CONTROL_FILE);

    private Map<String/*server name*/, Map<String/*flowControlType*/, List<FlowControlRule>>> plainFlowControlRules;

    public FlowControlConfig() {
        loadFlowControlConfig();
    }

    public void loadFlowControlConfig() {
        JSONObject jsonObject = UtilAll.getYamlDataObject(flowControlFileHome + File.separator + flowControlFileName,
            JSONObject.class);
        if (jsonObject != null && jsonObject.size() > 0) {
            plainFlowControlRules = new HashMap<String/*server name*/, Map<String /*flowControlType*/, List<FlowControlRule>>>();
            Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                String serverName = entry.getKey();
                Map<String /*flowControlType*/, List<FlowControlRule>> flowControlTypeMap = plainFlowControlRules.get(serverName);
                if (flowControlTypeMap == null) {
                    flowControlTypeMap = new HashMap<String, List<FlowControlRule>>();
                    plainFlowControlRules.put(serverName, flowControlTypeMap);
                }

                LinkedHashMap flowControlMap = (LinkedHashMap) entry.getValue();
                Iterator iter = flowControlMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry tmp = (Map.Entry) iter.next();
                    String flowControlType = (String) tmp.getKey();
                    List jsonList = (ArrayList) tmp.getValue();
                    if (jsonList != null) {
                        for (Object json : jsonList) {
                            Map map = (LinkedHashMap) json;
                            FlowControlRule flowControlRule = JSON.parseObject(JSON.toJSONString(map), FlowControlRule.class);
                            List<FlowControlRule> flowControlRules = flowControlTypeMap.get(flowControlType);
                            if (flowControlRules == null) {
                                flowControlRules = new ArrayList<FlowControlRule>();
                                flowControlTypeMap.put(flowControlType, flowControlRules);
                            }
                            flowControlRules.add(flowControlRule);
                        }
                    }
                }
            }
        }
        log.warn("Load topic config: {}", this.plainFlowControlRules);
    }

    public Map<String, Map<String, List<FlowControlRule>>> getPlainFlowControlRules() {
        return plainFlowControlRules;
    }

}
