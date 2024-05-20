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
package org.apache.rocketmq.ratelimit.model;

import java.util.Objects;

public class RatelimitRule {

    private String name;

    private EntityType entityType;

    private String entityName;

    private MatcherType matcherType;

    private double produceTps;

    private double consumeTps;

    public RatelimitRule() {
    }

    public RatelimitRule(RatelimitRule rule) {
        if (rule == null) {
            return;
        }
        this.name = rule.name;
        this.entityType = rule.entityType;
        this.entityName = rule.entityName;
        this.matcherType = rule.matcherType;
        this.produceTps = rule.produceTps;
        this.consumeTps = rule.consumeTps;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public MatcherType getMatcherType() {
        return matcherType;
    }

    public void setMatcherType(MatcherType matcherType) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RatelimitRule rule = (RatelimitRule) o;
        return Double.compare(produceTps, rule.produceTps) == 0 &&
                Double.compare(consumeTps, rule.consumeTps) == 0 &&
                Objects.equals(name, rule.name) &&
                entityType == rule.entityType &&
                Objects.equals(entityName, rule.entityName) &&
                matcherType == rule.matcherType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, entityType, entityName, matcherType, produceTps, consumeTps);
    }
}
