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
package org.apache.rocketmq.tools.admin.api;

import java.util.List;

public class BrokerOperatorResult {

    private List<String> successList;

    private List<String> failureList;

    public List<String> getSuccessList() {
        return successList;
    }

    public void setSuccessList(List<String> successList) {
        this.successList = successList;
    }

    public List<String> getFailureList() {
        return failureList;
    }

    public void setFailureList(List<String> failureList) {
        this.failureList = failureList;
    }

    @Override
    public String toString() {
        return "BrokerOperatorResult{" +
            "successList=" + successList +
            ", failureList=" + failureList +
            '}';
    }
}
