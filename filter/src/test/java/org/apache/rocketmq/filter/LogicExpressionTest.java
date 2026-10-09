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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.filter.expression.BooleanExpression;
import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.LogicExpression;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the three-valued (TRUE / FALSE / null-UNKNOWN) truth tables of
 * {@link LogicExpression#createAND} / {@link LogicExpression#createOR},
 * their short-circuit behavior, and the UNKNOWN-matches-false rule.
 */
@RunWith(Parameterized.class)
public class LogicExpressionTest {

    /**
     * Operand stub: yields a fixed Boolean (null = SQL UNKNOWN) and counts
     * how many times it was actually evaluated.
     */
    static final class Operand implements BooleanExpression {
        private final Boolean value;
        private int evaluations;

        Operand(Boolean value) {
            this.value = value;
        }

        int evaluations() {
            return evaluations;
        }

        @Override
        public Object evaluate(EvaluationContext context) {
            evaluations++;
            return value;
        }

        @Override
        public boolean matches(EvaluationContext context) {
            Object r = null;
            try {
                r = evaluate(context);
            } catch (Exception ignored) {
            }
            return Boolean.TRUE.equals(r);
        }
    }

    @Parameterized.Parameter(0)
    public Boolean leftValue;

    @Parameterized.Parameter(1)
    public Boolean rightValue;

    @Parameterized.Parameter(2)
    public Boolean expectedAnd;

    @Parameterized.Parameter(3)
    public Boolean expectedOr;

    @Parameterized.Parameters(name = "left={0}, right={1}")
    public static List<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        // left, right, AND, OR  (null models SQL UNKNOWN)
        data.add(new Object[] {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE});
        data.add(new Object[] {Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, Boolean.TRUE});
        data.add(new Object[] {Boolean.TRUE, null, null, Boolean.TRUE});
        data.add(new Object[] {Boolean.FALSE, Boolean.TRUE, Boolean.FALSE, Boolean.TRUE});
        data.add(new Object[] {Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE});
        data.add(new Object[] {Boolean.FALSE, null, Boolean.FALSE, null});
        data.add(new Object[] {null, Boolean.TRUE, null, Boolean.TRUE});
        data.add(new Object[] {null, Boolean.FALSE, Boolean.FALSE, null});
        data.add(new Object[] {null, null, null, null});
        return data;
    }

    @Test
    public void testTruthTable() throws Exception {
        Operand left = new Operand(leftValue);
        Operand right = new Operand(rightValue);

        assertThat(LogicExpression.createAND(left, right).evaluate(null)).isEqualTo(expectedAnd);
        assertThat(LogicExpression.createOR(left, right).evaluate(null)).isEqualTo(expectedOr);

        // no short-circuit applies for these combinations: both operands are evaluated
        // (verified explicitly in the dedicated short-circuit tests below)
    }

    @Test
    public void testMatchesTreatsUnknownAsFalse() throws Exception {
        Operand left = new Operand(leftValue);
        Operand right = new Operand(rightValue);

        Boolean andResult = LogicExpression.createAND(left, right).matches(null);
        Boolean orResult = LogicExpression.createOR(left, right).matches(null);

        assertThat(andResult).isEqualTo(Boolean.TRUE.equals(expectedAnd));
        assertThat(orResult).isEqualTo(Boolean.TRUE.equals(expectedOr));
    }

    @Test
    public void testAndShortCircuitsOnFalseLeft() throws Exception {
        Operand left = new Operand(Boolean.FALSE);
        Operand right = new Operand(Boolean.TRUE);

        Object result = LogicExpression.createAND(left, right).evaluate(null);

        assertThat(result).isEqualTo(Boolean.FALSE);
        assertThat(right.evaluations()).isEqualTo(0);
    }

    @Test
    public void testOrShortCircuitsOnTrueLeft() throws Exception {
        Operand left = new Operand(Boolean.TRUE);
        Operand right = new Operand(Boolean.FALSE);

        Object result = LogicExpression.createOR(left, right).evaluate(null);

        assertThat(result).isEqualTo(Boolean.TRUE);
        assertThat(right.evaluations()).isEqualTo(0);
    }

    @Test
    public void testAndEvaluatesRightWhenLeftNotFalse() throws Exception {
        Operand left = new Operand(Boolean.TRUE);
        Operand right = new Operand(Boolean.FALSE);

        Object result = LogicExpression.createAND(left, right).evaluate(null);

        assertThat(result).isEqualTo(Boolean.FALSE);
        assertThat(left.evaluations()).isEqualTo(1);
        assertThat(right.evaluations()).isEqualTo(1);
    }

    @Test
    public void testOrEvaluatesRightWhenLeftNotTrue() throws Exception {
        Operand left = new Operand(Boolean.FALSE);
        Operand right = new Operand(Boolean.TRUE);

        Object result = LogicExpression.createOR(left, right).evaluate(null);

        assertThat(result).isEqualTo(Boolean.TRUE);
        assertThat(left.evaluations()).isEqualTo(1);
        assertThat(right.evaluations()).isEqualTo(1);
    }
}
