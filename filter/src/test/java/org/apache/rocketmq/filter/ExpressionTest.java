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

import org.apache.rocketmq.filter.expression.ComparisonExpression;
import org.apache.rocketmq.filter.expression.ConstantExpression;
import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.apache.rocketmq.filter.expression.PropertyExpression;
import org.apache.rocketmq.filter.parser.SelectorParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest {

    private static String andExpression = "a=3 and b<>4 And c>5 AND d<=4";
    private static String orExpression = "a=3 or b<>4 Or c>5 OR d<=4";
    private static String inExpression = "a in ('3', '4', '5')";
    private static String notInExpression = "a not in ('3', '4', '5')";
    private static String betweenExpression = "a between 2 and 10";
    private static String notBetweenExpression = "a not between 2 and 10";
    private static String isNullExpression = "a is null";
    private static String isNotNullExpression = "a is not null";
    private static String equalExpression = "a is not null and a='hello'";
    private static String booleanExpression = "a=TRUE OR b=FALSE";
    private static String nullOrExpression = "a is null OR a='hello'";
    private static String stringHasString = "TAGS is not null and TAGS='''''tag'''''";

    @Test
    public void testEvaluate_stringHasString() throws Exception {
        Expression expr = genExp(stringHasString);

        EvaluationContext context = genContext(
            KeyValue.c("TAGS", "''tag''")
        );

        eval(expr, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_now() throws Exception {
        EvaluationContext context = genContext(
            KeyValue.c("a", System.currentTimeMillis())
        );

        Expression nowExpression = ConstantExpression.createNow();
        Expression propertyExpression = new PropertyExpression("a");

        Expression expression = ComparisonExpression.createLessThanEqual(propertyExpression,
            nowExpression);

        eval(expression, context, Boolean.TRUE);
    }

    @Test(expected = RuntimeException.class)
    public void testEvaluate_stringCompare() throws Exception {
        Expression expression = genExp("a between up and low");

        EvaluationContext context = genContext(
            KeyValue.c("a", "3.14")
        );

        eval(expression, context, Boolean.FALSE);

        {
            context = genContext(
                KeyValue.c("a", "3.14"),
                KeyValue.c("up", "up"),
                KeyValue.c("low", "low")
            );

            eval(expression, context, Boolean.FALSE);
        }

        {
            expression = genExp("key is not null and key between 0 and 100");

            context = genContext(
                KeyValue.c("key", "con")
            );

            eval(expression, context, Boolean.FALSE);
        }

        {
            expression = genExp("a between 0 and 100");

            context = genContext(
                KeyValue.c("a", "abc")
            );

            eval(expression, context, Boolean.FALSE);
        }

        {
            expression = genExp("a=b");

            context = genContext(
                KeyValue.c("a", "3.14"),
                KeyValue.c("b", "3.14")
            );

            eval(expression, context, Boolean.TRUE);
        }

        {
            expression = genExp("a<>b");

            context = genContext(
                KeyValue.c("a", "3.14"),
                KeyValue.c("b", "3.14")
            );

            eval(expression, context, Boolean.FALSE);
        }

        {
            expression = genExp("a<>b");

            context = genContext(
                KeyValue.c("a", "3.14"),
                KeyValue.c("b", "3.141")
            );

            eval(expression, context, Boolean.TRUE);
        }
    }

    @Test
    public void testEvaluate_exponent() throws Exception {
        Expression expression = genExp("a > 3.1E10");

        EvaluationContext context = genContext(
            KeyValue.c("a", String.valueOf(3.1415 * Math.pow(10, 10)))
        );

        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_floatNumber() throws Exception {
        Expression expression = genExp("a > 3.14");

        EvaluationContext context = genContext(
            KeyValue.c("a", String.valueOf(3.1415))
        );

        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_twoVariable() throws Exception {
        Expression expression = genExp("a > b");

        EvaluationContext context = genContext(
            KeyValue.c("a", String.valueOf(10)),
            KeyValue.c("b", String.valueOf(20))
        );

        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_twoVariableGt() throws Exception {
        Expression expression = genExp("a > b");
        EvaluationContext context = genContext(
            KeyValue.c("b", String.valueOf(10)),
            KeyValue.c("a", String.valueOf(20))
        );

        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_nullOr() throws Exception {
        Expression expression = genExp(nullOrExpression);

        EvaluationContext context = genContext(
        );

        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "hello")
        );

        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "abc")
        );

        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_boolean() throws Exception {
        Expression expression = genExp(booleanExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "true"),
            KeyValue.c("b", "false")
        );

        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "false"),
            KeyValue.c("b", "true")
        );

        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_equal() throws Exception {
        Expression expression = genExp(equalExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "hello")
        );

        eval(expression, context, Boolean.TRUE);

        context = genContext(
        );

        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_andTrue() throws Exception {
        Expression expression = genExp(andExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", 3),
            KeyValue.c("b", 5),
            KeyValue.c("c", 6),
            KeyValue.c("d", 1)
        );

        for (int i = 0; i < 500; i++) {
            eval(expression, context, Boolean.TRUE);
        }

        long start = System.currentTimeMillis();
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 1000; i++) {
                eval(expression, context, Boolean.TRUE);
            }
        }

        // use string
        context = genContext(
            KeyValue.c("a", "3"),
            KeyValue.c("b", "5"),
            KeyValue.c("c", "6"),
            KeyValue.c("d", "1")
        );

        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_andFalse() throws Exception {
        Expression expression = genExp(andExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", 4),
            KeyValue.c("b", 5),
            KeyValue.c("c", 6),
            KeyValue.c("d", 1)
        );

        eval(expression, context, Boolean.FALSE);

        // use string
        context = genContext(
            KeyValue.c("a", "4"),
            KeyValue.c("b", "5"),
            KeyValue.c("c", "6"),
            KeyValue.c("d", "1")
        );

        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_orTrue() throws Exception {
        Expression expression = genExp(orExpression);

        // first
        EvaluationContext context = genContext(
            KeyValue.c("a", 3)
        );
        eval(expression, context, Boolean.TRUE);

        // second
        context = genContext(
            KeyValue.c("a", 4),
            KeyValue.c("b", 5)
        );
        eval(expression, context, Boolean.TRUE);

        // third
        context = genContext(
            KeyValue.c("a", 4),
            KeyValue.c("b", 4),
            KeyValue.c("c", 6)
        );
        eval(expression, context, Boolean.TRUE);

        // forth
        context = genContext(
            KeyValue.c("a", 4),
            KeyValue.c("b", 4),
            KeyValue.c("c", 3),
            KeyValue.c("d", 2)
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_orFalse() throws Exception {
        Expression expression = genExp(orExpression);
        // forth
        EvaluationContext context = genContext(
            KeyValue.c("a", 4),
            KeyValue.c("b", 4),
            KeyValue.c("c", 3),
            KeyValue.c("d", 10)
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_inTrue() throws Exception {
        Expression expression = genExp(inExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "3")
        );
        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "4")
        );
        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "5")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_inFalse() throws Exception {
        Expression expression = genExp(inExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "8")
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_notInTrue() throws Exception {
        Expression expression = genExp(notInExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "8")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_notInFalse() throws Exception {
        Expression expression = genExp(notInExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "3")
        );
        eval(expression, context, Boolean.FALSE);

        context = genContext(
            KeyValue.c("a", "4")
        );
        eval(expression, context, Boolean.FALSE);

        context = genContext(
            KeyValue.c("a", "5")
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_betweenTrue() throws Exception {
        Expression expression = genExp(betweenExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "2")
        );
        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "10")
        );
        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "3")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_betweenFalse() throws Exception {
        Expression expression = genExp(betweenExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "1")
        );
        eval(expression, context, Boolean.FALSE);

        context = genContext(
            KeyValue.c("a", "11")
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_notBetweenTrue() throws Exception {
        Expression expression = genExp(notBetweenExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "1")
        );
        eval(expression, context, Boolean.TRUE);

        context = genContext(
            KeyValue.c("a", "11")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_notBetweenFalse() throws Exception {
        Expression expression = genExp(notBetweenExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "2")
        );
        eval(expression, context, Boolean.FALSE);

        context = genContext(
            KeyValue.c("a", "10")
        );
        eval(expression, context, Boolean.FALSE);

        context = genContext(
            KeyValue.c("a", "3")
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_isNullTrue() throws Exception {
        Expression expression = genExp(isNullExpression);

        EvaluationContext context = genContext(
            KeyValue.c("abc", "2")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_isNullFalse() throws Exception {
        Expression expression = genExp(isNullExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "2")
        );
        eval(expression, context, Boolean.FALSE);
    }

    @Test
    public void testEvaluate_isNotNullTrue() throws Exception {
        Expression expression = genExp(isNotNullExpression);

        EvaluationContext context = genContext(
            KeyValue.c("a", "2")
        );
        eval(expression, context, Boolean.TRUE);
    }

    @Test
    public void testEvaluate_isNotNullFalse() throws Exception {
        Expression expression = genExp(isNotNullExpression);

        EvaluationContext context = genContext(
            KeyValue.c("abc", "2")
        );
        eval(expression, context, Boolean.FALSE);
    }

    protected void eval(Expression expression, EvaluationContext context, Boolean result) throws Exception {
        Object ret = expression.evaluate(context);
        if (ret == null || !(ret instanceof Boolean)) {
            assertThat(result).isFalse();
        } else {
            assertThat(result).isEqualTo(ret);
        }
    }

    protected EvaluationContext genContext(KeyValue... keyValues) {
        if (keyValues == null || keyValues.length < 1) {
            return new PropertyContext();
        }

        PropertyContext context = new PropertyContext();
        for (KeyValue keyValue : keyValues) {
            context.properties.put(keyValue.key, keyValue.value);
        }

        return context;
    }

    protected Expression genExp(String exp) {
        Expression expression = null;

        try {
            expression = SelectorParser.parse(exp);

            assertThat(expression).isNotNull();
        } catch (MQFilterException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        return expression;
    }

    static class KeyValue {
        public static KeyValue c(String key, Object value) {
            return new KeyValue(key, value);
        }

        public KeyValue(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key;
        public Object value;
    }

    class PropertyContext implements EvaluationContext {

        public Map<String, Object> properties = new HashMap<String, Object>(8);

        @Override
        public Object get(final String name) {
            return properties.get(name);
        }

        @Override
        public Map<String, Object> keyValues() {
            return properties;
        }

    }
}
