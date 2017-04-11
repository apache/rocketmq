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

package org.apache.rocketmq.filter;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.apache.rocketmq.filter.parser.SelectorParser;

/**
 * SQL92 Filter, just a wrapper of {@link org.apache.rocketmq.filter.parser.SelectorParser}.
 * <p/>
 * <p>
 * Do not use this filter directly.Use {@link FilterFactory#get} to select a filter.
 * </p>
 */
public class SqlFilter implements FilterSpi {

    @Override
    public Expression compile(final String expr) throws MQFilterException {
        return SelectorParser.parse(expr);
    }

    @Override
    public String ofType() {
        return ExpressionType.SQL92;
    }
}
