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

import org.apache.rocketmq.filter.constant.UnaryType;

import java.util.Collection;

/**
 * In expression.
 */
abstract public class UnaryInExpression extends UnaryExpression implements BooleanExpression {

    private boolean not;

    private Collection inList;

    public UnaryInExpression(Expression left, UnaryType unaryType,
        Collection inList, boolean not) {
        super(left, unaryType);
        this.setInList(inList);
        this.setNot(not);

    }

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public Collection getInList() {
        return inList;
    }

    public void setInList(Collection inList) {
        this.inList = inList;
    }
}
