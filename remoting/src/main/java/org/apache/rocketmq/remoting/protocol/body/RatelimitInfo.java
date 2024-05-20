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
package org.apache.rocketmq.remoting.protocol.body;

public class RatelimitInfo {

    private String name;

    private String entityType;

    private String entityName;

    private String matcherType;

    private double produceTps;

    private double consumeTps;

    public RatelimitInfo(String name, String entityType, String entityName, String matcherType, double produceTps, double consumeTps) {
        this.name = name;
        this.entityType = entityType;
        this.entityName = entityName;
        this.matcherType = matcherType;
        this.produceTps = produceTps;
        this.consumeTps = consumeTps;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getMatcherType() {
        return matcherType;
    }

    public void setMatcherType(String matcherType) {
        this.matcherType = matcherType;
    }

    public double getProduceTps() {
        return produceTps;
    }

    public void setProduceTps(double produceTps) {
        this.produceTps = produceTps;
    }

    public double getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(double consumeTps) {
        this.consumeTps = consumeTps;
    }
}
