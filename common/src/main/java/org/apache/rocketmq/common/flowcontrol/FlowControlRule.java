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

public class FlowControlRule {
    private String flowControlResourceName;
    private Integer flowControlGrade;
    private Integer flowControlBehavior;
    private double flowControlResourceCount;

    public String getFlowControlResourceName() {
        return flowControlResourceName;
    }

    public void setFlowControlResourceName(String flowControlResourceName) {
        this.flowControlResourceName = flowControlResourceName;
    }

    public Integer getFlowControlGrade() {
        return flowControlGrade;
    }

    public void setFlowControlGrade(Integer flowControlGrade) {
        this.flowControlGrade = flowControlGrade;
    }

    public Integer getFlowControlBehavior() {
        return flowControlBehavior;
    }

    public void setFlowControlBehavior(Integer flowControlBehavior) {
        this.flowControlBehavior = flowControlBehavior;
    }

    public double getFlowControlResourceCount() {
        return flowControlResourceCount;
    }

    public void setFlowControlResourceCount(double flowControlResourceCount) {
        this.flowControlResourceCount = flowControlResourceCount;
    }

    @Override
    public String toString() {
        return "FlowControlRule{" +
            "flowControlResourceName='" + flowControlResourceName + '\'' +
            ", flowControlGrade=" + flowControlGrade +
            ", flowControlBehavior=" + flowControlBehavior +
            ", flowControlResourceCount=" + flowControlResourceCount +
            '}';
    }
}
