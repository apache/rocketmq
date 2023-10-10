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

import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.apache.rocketmq.filter.parser.SelectorParser;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ParserTest {

    private static String andExpression = "a=3 and b<>4 And c>5 AND d<=4";
    private static String andExpressionHasBlank = "a=3  and    b<>4 And c>5 AND d<=4";
    private static String orExpression = "a=3 or b<>4 Or c>5 OR d<=4";
    private static String inExpression = "a in ('3', '4', '5')";
    private static String notInExpression = "(a not in ('6', '4', '5')) or (b in ('3', '4', '5'))";
    private static String betweenExpression = "(a between 2 and 10) AND (b not between 6 and 9)";
    private static String equalNullExpression = "a is null";
    private static String notEqualNullExpression = "a is not null";
    private static String nowExpression = "a <= now";
    private static String containsExpression = "a=3 and b contains 'xxx' and c not contains 'xxx'";
    private static String invalidExpression = "a and between 2 and 10";
    private static String illegalBetween = " a between 10 and 0";

    @Test
    public void testParse_valid() {
        for (String expr : Arrays.asList(
            andExpression, orExpression, inExpression, notInExpression, betweenExpression,
            equalNullExpression, notEqualNullExpression, nowExpression, containsExpression
        )) {

            try {
                Expression expression = SelectorParser.parse(expr);
                assertThat(expression).isNotNull();
            } catch (MQFilterException e) {
                e.printStackTrace();
                assertThat(Boolean.FALSE).isTrue();
            }

        }
    }

    @Test
    public void testParse_invalid() {
        try {
            SelectorParser.parse(invalidExpression);

            assertThat(Boolean.TRUE).isFalse();
        } catch (MQFilterException e) {
        }
    }

    @Test
    public void testParse_decimalOverFlow() {
        try {
            String str = "100000000000000000000000";

            SelectorParser.parse("a > " + str);

            assertThat(Boolean.TRUE).isFalse();
        } catch (Exception e) {
        }
    }

    @Test
    public void testParse_floatOverFlow() {
        try {
            StringBuilder sb = new StringBuilder(210000);
            sb.append("1");
            for (int i = 0; i < 2048; i ++) {
                sb.append("111111111111111111111111111111111111111111111111111");
            }
            sb.append(".");
            for (int i = 0; i < 2048; i ++) {
                sb.append("111111111111111111111111111111111111111111111111111");
            }
            String str = sb.toString();


            SelectorParser.parse("a > " + str);

            assertThat(Boolean.TRUE).isFalse();
        } catch (Exception e) {
        }
    }

    @Test
    public void testParse_illegalBetween() {
        try {
            SelectorParser.parse(illegalBetween);

            assertThat(Boolean.TRUE).isFalse();
        } catch (Exception e) {
        }
    }

    @Test
    public void testEquals() {
        try {
            Expression expr1 = SelectorParser.parse(andExpression);

            Expression expr2 = SelectorParser.parse(andExpressionHasBlank);

            Expression expr3 = SelectorParser.parse(orExpression);

            assertThat(expr1).isEqualTo(expr2);
            assertThat(expr1).isNotEqualTo(expr3);
        } catch (MQFilterException e) {
            e.printStackTrace();
            assertThat(Boolean.TRUE).isFalse();
        }
    }
}
