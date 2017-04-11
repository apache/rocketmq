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

package org.apache.rocketmq.filter.expression;

/**
 * Represents a property expression
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.PropertyExpression,
 * but more simple and no transfer between expression and message property.
 * </p>
 */
public class PropertyExpression implements Expression {
    private final String name;

    public PropertyExpression(String name) {
        this.name = name;
    }

    @Override
    public Object evaluate(EvaluationContext context) throws Exception {
        return context.get(name);
    }

    public String getName() {
        return name;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);
    }
}
