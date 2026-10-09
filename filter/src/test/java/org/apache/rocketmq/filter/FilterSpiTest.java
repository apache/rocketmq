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
import org.apache.rocketmq.filter.expression.EmptyEvaluationContext;
import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterSpiTest {

    static class NothingExpression implements Expression {

        @Override
        public Object evaluate(final EvaluationContext context) throws Exception {
            return Boolean.TRUE;
        }
    }

    static class NothingFilter implements FilterSpi {
        @Override
        public Expression compile(final String expr) throws MQFilterException {
            return new NothingExpression();
        }

        @Override
        public String ofType() {
            return "Nothing";
        }
    }

    @Test
    public void testRegister() {
        FilterFactory.INSTANCE.register(new NothingFilter());

        Expression expr = null;
        try {
            expr = FilterFactory.INSTANCE.get("Nothing").compile("abc");
        } catch (MQFilterException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        assertThat(expr).isNotNull();

        try {
            assertThat((Boolean) expr.evaluate(new EmptyEvaluationContext())).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        FilterFactory.INSTANCE.unRegister("Nothing");
    }

    @Test
    public void testGet() {
        try {
            assertThat((Boolean) FilterFactory.INSTANCE.get(ExpressionType.SQL92).compile("a is not null and a > 0")
                .evaluate(new EmptyEvaluationContext())).isFalse();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    static class AnotherNothingFilter implements FilterSpi {
        @Override
        public Expression compile(final String expr) throws MQFilterException {
            throw new MQFilterException("never compiled in this test", null);
        }

        @Override
        public String ofType() {
            return "Nothing";
        }
    }

    @Test
    public void testRegisterDuplicateTypeIsRejectedAndKeepsOriginal() throws Exception {
        FilterFactory.INSTANCE.unRegister("Nothing");
        try {
            FilterFactory.INSTANCE.register(new NothingFilter());

            try {
                FilterFactory.INSTANCE.register(new AnotherNothingFilter());
                assertThat(Boolean.FALSE).isTrue();
            } catch (IllegalArgumentException expected) {
                assertThat(expected.getMessage()).contains("Nothing");
            }

            // the original registration must survive the rejected duplicate
            assertThat(FilterFactory.INSTANCE.get("Nothing")).isInstanceOf(NothingFilter.class);
            assertThat(FilterFactory.INSTANCE.get("Nothing").compile("abc")).isNotNull();
        } finally {
            FilterFactory.INSTANCE.unRegister("Nothing");
        }
    }

    @Test
    public void testUnRegisterReturnsFilterAndAllowsReRegistration() throws Exception {
        FilterFactory.INSTANCE.unRegister("Nothing");
        try {
            FilterFactory.INSTANCE.register(new NothingFilter());

            assertThat(FilterFactory.INSTANCE.unRegister("Nothing")).isInstanceOf(NothingFilter.class);
            assertThat(FilterFactory.INSTANCE.get("Nothing")).isNull();

            // after unregistering, the type can be registered again
            FilterFactory.INSTANCE.register(new AnotherNothingFilter());
            assertThat(FilterFactory.INSTANCE.get("Nothing")).isInstanceOf(AnotherNothingFilter.class);
        } finally {
            FilterFactory.INSTANCE.unRegister("Nothing");
        }
    }

    @Test
    public void testDuplicateBuiltInSqlTypeIsRejected() {
        try {
            FilterFactory.INSTANCE.register(new org.apache.rocketmq.filter.SqlFilter());
            assertThat(Boolean.FALSE).isTrue();
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains(ExpressionType.SQL92);
        }
        // the built-in filter must remain functional
        try {
            assertThat(FilterFactory.INSTANCE.get(ExpressionType.SQL92)).isNotNull();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }
}
