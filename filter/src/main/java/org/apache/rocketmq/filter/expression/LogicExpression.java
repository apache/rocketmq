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
 * A filter performing a comparison of two objects
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.LogicExpression,
 * </p>
 */
public abstract class LogicExpression extends BinaryExpression implements BooleanExpression {

    /**
     * @param left
     * @param right
     */
    public LogicExpression(BooleanExpression left, BooleanExpression right) {
        super(left, right);
    }

    public static BooleanExpression createOR(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                Boolean lv = (Boolean) left.evaluate(context);
                if (lv != null && lv.booleanValue()) {
                    return Boolean.TRUE;
                }
                Boolean rv = (Boolean) right.evaluate(context);
                if (rv != null && rv.booleanValue()) {
                    return Boolean.TRUE;
                }
                if (lv == null || rv == null) {
                    return null;
                }
                return Boolean.FALSE;
            }

            public String getExpressionSymbol() {
                return "||";
            }
        };
    }

    public static BooleanExpression createAND(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                Boolean lv = (Boolean) left.evaluate(context);

                if (lv != null && !lv.booleanValue()) {
                    return Boolean.FALSE;
                }
                Boolean rv = (Boolean) right.evaluate(context);
                if (rv != null && !rv.booleanValue()) {
                    return Boolean.FALSE;
                }
                if (lv == null || rv == null) {
                    return null;
                }
                return Boolean.TRUE;
            }

            public String getExpressionSymbol() {
                return "&&";
            }
        };
    }

    public abstract Object evaluate(EvaluationContext context) throws Exception;

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

}
