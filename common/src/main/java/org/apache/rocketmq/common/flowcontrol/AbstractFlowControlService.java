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

import com.alibaba.csp.sentinel.SphO;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.interceptor.ExceptionContext;
import org.apache.rocketmq.remoting.interceptor.Interceptor;
import org.apache.rocketmq.remoting.interceptor.RequestContext;
import org.apache.rocketmq.remoting.interceptor.ResponseContext;

public abstract class AbstractFlowControlService implements Interceptor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private final ThreadLocal<Boolean> acquiredThreadLocal = new ThreadLocal<Boolean>();
    private final FlowControlConfig flowControlConfig;

    public final String flowControlNameSeparator = "@";

    public AbstractFlowControlService() {
        this.flowControlConfig = new FlowControlConfig();
        loadRules(this.flowControlConfig);
    }

    public abstract String getResourceName(RequestContext requestContext);

    public abstract int getResourceCount(RequestContext requestContext);

    public abstract String getFlowControlType();

    public abstract void rejectRequest(RequestContext requestContext);

    @Override
    public void beforeRequest(RequestContext requestContext) {
        String resourceName = getResourceName(requestContext);
        int resourceCount = getResourceCount(requestContext);
        if (resourceName != null && resourceCount > 0) {
            String flowControlType = getFlowControlType();
            String resourceKey = buildResourceKey(flowControlType, resourceName);
            log.debug("resourceKey: {} resourceCount: {}", resourceKey, resourceCount);
            boolean acquired = SphO.entry(resourceKey, resourceCount);
            if (acquired) {
                this.acquiredThreadLocal.set(true);
            } else {
                rejectRequest(requestContext);
            }
        }
    }

    @Override
    public void afterRequest(ResponseContext responseContext) {
        Boolean acquired = this.acquiredThreadLocal.get();
        if (acquired != null && acquired) {
            SphO.exit();
            this.acquiredThreadLocal.remove();
        }
    }

    @Override
    public void onException(ExceptionContext exceptionContext) {
        Boolean acquired = this.acquiredThreadLocal.get();
        if (acquired != null && acquired) {
            SphO.exit();
            this.acquiredThreadLocal.remove();
        }
    }

    public List<FlowControlRule> getRules(String moduleName, String flowControlType) {
        if (this.flowControlConfig != null) {
            Map<String, Map<String, List<FlowControlRule>>> rules = this.flowControlConfig.getPlainFlowControlRules();
            Map<String, List<FlowControlRule>> flowControlMap = rules.get(moduleName);
            if (flowControlMap != null) {
                if (flowControlMap.get(flowControlType) != null) {
                    return flowControlMap.get(flowControlType);
                } else {
                    log.warn("Get flow control config null by flowControlType: {} ", flowControlType);
                }
            } else {
                log.warn("Get flow control config null by moduleName: {} ", moduleName);
            }
        } else {
            log.warn("FlowControlConfig is null ");
        }
        return null;
    }

    private String buildResourceKey(String flowControlType, String flowControlResourceName) {
        StringBuffer sb = new StringBuffer(32);
        sb.append(flowControlType).append(flowControlNameSeparator).append(flowControlResourceName);
        return sb.toString();
    }

    private void loadRules(FlowControlConfig flowControlConfig) {
        Map<String, Map<String, List<FlowControlRule>>> rules = flowControlConfig.getPlainFlowControlRules();
        List<FlowRule> sentinelRules = new ArrayList<FlowRule>();
        for (Map<String, List<FlowControlRule>> rulesMap : rules.values()) {
            Set<Map.Entry<String, List<FlowControlRule>>> entrySet = rulesMap.entrySet();
            Iterator iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<FlowControlRule>> entry = (Map.Entry<String, List<FlowControlRule>>) iterator.next();
                String flowControlType = entry.getKey();
                List<FlowControlRule> list = entry.getValue();
                for (FlowControlRule flowControlRule : list) {
                    FlowRule rule1 = new FlowRule();
                    rule1.setResource(buildResourceKey(flowControlType, flowControlRule.getFlowControlResourceName()));
                    rule1.setCount(flowControlRule.getFlowControlResourceCount());
                    rule1.setGrade(flowControlRule.getFlowControlGrade());
                    rule1.setLimitApp("default");
                    sentinelRules.add(rule1);
                }
            }
        }
        FlowRuleManager.loadRules(sentinelRules);
        log.warn("Load Rules: {}", FlowRuleManager.getRules());
    }
}

