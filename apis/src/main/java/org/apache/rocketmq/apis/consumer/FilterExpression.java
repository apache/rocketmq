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


package org.apache.rocketmq.apis.consumer;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterExpression {
    public static final String TAG_EXPRESSION_SUB_ALL = "*";
    public static final String TAG_EXPRESSION_SPLITTER = "\\|\\|";
    private final String expression;
    private final FilterExpressionType filterExpressionType;

    public FilterExpression(String expression, FilterExpressionType filterExpressionType) {
        this.expression = checkNotNull(expression, "expression should not be null");
        this.filterExpressionType = checkNotNull(filterExpressionType, "filterExpressionType should not be null");
    }

    public String getExpression() {
        return expression;
    }

    public FilterExpressionType getFilterExpressionType() {
        return filterExpressionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilterExpression that = (FilterExpression) o;
        return expression.equals(that.expression) && filterExpressionType == that.filterExpressionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, filterExpressionType);
    }
}
