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

import java.util.List;

/**
 * A filter performing a comparison of two objects
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.ComparisonExpression,
 * but:
 * 1. Remove LIKE expression, and related methods;
 * 2. Extract a new method __compare which has int return value;
 * 3. When create between expression, check whether left value is less or equal than right value;
 * 4. For string type value(can not convert to number), only equal or unequal comparison are supported.
 * </p>
 */
public abstract class ComparisonExpression extends BinaryExpression implements BooleanExpression {

    public static final ThreadLocal<Boolean> CONVERT_STRING_EXPRESSIONS = new ThreadLocal<>();

    boolean convertStringExpressions = false;

    /**
     * @param left
     * @param right
     */
    public ComparisonExpression(Expression left, Expression right) {
        super(left, right);
        convertStringExpressions = CONVERT_STRING_EXPRESSIONS.get() != null;
    }

    public static BooleanExpression createBetween(Expression value, Expression left, Expression right) {
        // check
        if (left instanceof ConstantExpression && right instanceof ConstantExpression) {
            Object lv = ((ConstantExpression) left).getValue();
            Object rv = ((ConstantExpression) right).getValue();
            if (lv == null || rv == null) {
                throw new RuntimeException("Illegal values of between, values can not be null!");
            }
            if (lv instanceof Comparable && rv instanceof Comparable) {
                int ret = __compare((Comparable) rv, (Comparable) lv, true);
                if (ret < 0)
                    throw new RuntimeException(
                        String.format("Illegal values of between, left value(%s) must less than or equal to right value(%s)", lv, rv)
                    );
            }
        }

        return LogicExpression.createAND(createGreaterThanEqual(value, left), createLessThanEqual(value, right));
    }

    public static BooleanExpression createNotBetween(Expression value, Expression left, Expression right) {
        return LogicExpression.createOR(createLessThan(value, left), createGreaterThan(value, right));
    }

    static class ContainsExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public ContainsExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "CONTAINS";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).contains(search) ? Boolean.TRUE : Boolean.FALSE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    static class NotContainsExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public NotContainsExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "NOT CONTAINS";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).contains(search) ? Boolean.FALSE : Boolean.TRUE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    public static BooleanExpression createContains(Expression left, String search) {
        return new ContainsExpression(left, search);
    }

    public static BooleanExpression createNotContains(Expression left, String search) {
        return new NotContainsExpression(left, search);
    }

    static class StartsWithExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public StartsWithExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "STARTSWITH";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).startsWith(search) ? Boolean.TRUE : Boolean.FALSE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    static class NotStartsWithExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public NotStartsWithExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "NOT STARTSWITH";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).startsWith(search) ? Boolean.FALSE : Boolean.TRUE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    public static BooleanExpression createStartsWith(Expression left, String search) {
        return new StartsWithExpression(left, search);
    }

    public static BooleanExpression createNotStartsWith(Expression left, String search) {
        return new NotStartsWithExpression(left, search);
    }

    static class EndsWithExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public EndsWithExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "ENDSWITH";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).endsWith(search) ? Boolean.TRUE : Boolean.FALSE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    static class NotEndsWithExpression extends UnaryExpression implements BooleanExpression {

        String search;

        public NotEndsWithExpression(Expression right, String search) {
            super(right);
            this.search = search;
        }

        public String getExpressionSymbol() {
            return "NOT ENDSWITH";
        }

        public Object evaluate(EvaluationContext message) throws Exception {

            if (search == null || search.length() == 0) {
                return Boolean.FALSE;
            }

            Object rv = this.getRight().evaluate(message);

            if (rv == null) {
                return Boolean.FALSE;
            }

            if (!(rv instanceof String)) {
                return Boolean.FALSE;
            }

            return ((String)rv).endsWith(search) ? Boolean.FALSE : Boolean.TRUE;
        }

        public boolean matches(EvaluationContext message) throws Exception {
            Object object = evaluate(message);
            return object != null && object == Boolean.TRUE;
        }
    }

    public static BooleanExpression createEndsWith(Expression left, String search) {
        return new EndsWithExpression(left, search);
    }

    public static BooleanExpression createNotEndsWith(Expression left, String search) {
        return new NotEndsWithExpression(left, search);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static BooleanExpression createInFilter(Expression left, List elements) {

        if (!(left instanceof PropertyExpression)) {
            throw new RuntimeException("Expected a property for In expression, got: " + left);
        }
        return UnaryExpression.createInExpression((PropertyExpression) left, elements, false);

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static BooleanExpression createNotInFilter(Expression left, List elements) {

        if (!(left instanceof PropertyExpression)) {
            throw new RuntimeException("Expected a property for In expression, got: " + left);
        }
        return UnaryExpression.createInExpression((PropertyExpression) left, elements, true);

    }

    public static BooleanExpression createIsNull(Expression left) {
        return doCreateEqual(left, BooleanConstantExpression.NULL);
    }

    public static BooleanExpression createIsNotNull(Expression left) {
        return UnaryExpression.createNOT(doCreateEqual(left, BooleanConstantExpression.NULL));
    }

    public static BooleanExpression createNotEqual(Expression left, Expression right) {
        return UnaryExpression.createNOT(createEqual(left, right));
    }

    public static BooleanExpression createEqual(Expression left, Expression right) {
        checkEqualOperand(left);
        checkEqualOperand(right);
        checkEqualOperandCompatability(left, right);
        return doCreateEqual(left, right);
    }

    @SuppressWarnings({"rawtypes"})
    private static BooleanExpression doCreateEqual(Expression left, Expression right) {
        return new ComparisonExpression(left, right) {

            public Object evaluate(EvaluationContext context) throws Exception {
                Object lv = left.evaluate(context);
                Object rv = right.evaluate(context);

                // If one of the values is null
                if (lv == null ^ rv == null) {
                    if (lv == null) {
                        return null;
                    }
                    return Boolean.FALSE;
                }
                if (lv == rv || lv.equals(rv)) {
                    return Boolean.TRUE;
                }
                if (lv instanceof Comparable && rv instanceof Comparable) {
                    return compare((Comparable) lv, (Comparable) rv);
                }
                return Boolean.FALSE;
            }

            protected boolean asBoolean(int answer) {
                return answer == 0;
            }

            public String getExpressionSymbol() {
                return "==";
            }
        };
    }

    public static BooleanExpression createGreaterThan(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {
            protected boolean asBoolean(int answer) {
                return answer > 0;
            }

            public String getExpressionSymbol() {
                return ">";
            }
        };
    }

    public static BooleanExpression createGreaterThanEqual(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {
            protected boolean asBoolean(int answer) {
                return answer >= 0;
            }

            public String getExpressionSymbol() {
                return ">=";
            }
        };
    }

    public static BooleanExpression createLessThan(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {

            protected boolean asBoolean(int answer) {
                return answer < 0;
            }

            public String getExpressionSymbol() {
                return "<";
            }

        };
    }

    public static BooleanExpression createLessThanEqual(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {

            protected boolean asBoolean(int answer) {
                return answer <= 0;
            }

            public String getExpressionSymbol() {
                return "<=";
            }
        };
    }

    /**
     * Only Numeric expressions can be used in >, >=, < or <= expressions.s
     */
    public static void checkLessThanOperand(Expression expr) {
        if (expr instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expr).getValue();
            if (value instanceof Number) {
                return;
            }

            // Else it's boolean or a String..
            throw new RuntimeException("Value '" + expr + "' cannot be compared.");
        }
        if (expr instanceof BooleanExpression) {
            throw new RuntimeException("Value '" + expr + "' cannot be compared.");
        }
    }

    /**
     * Validates that the expression can be used in == or <> expression. Cannot
     * not be NULL TRUE or FALSE literals.
     */
    public static void checkEqualOperand(Expression expr) {
        if (expr instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expr).getValue();
            if (value == null) {
                throw new RuntimeException("'" + expr + "' cannot be compared.");
            }
        }
    }

    /**
     * @param left
     * @param right
     */
    private static void checkEqualOperandCompatability(Expression left, Expression right) {
        if (left instanceof ConstantExpression && right instanceof ConstantExpression) {
            if (left instanceof BooleanExpression && !(right instanceof BooleanExpression)) {
                throw new RuntimeException("'" + left + "' cannot be compared with '" + right + "'");
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object evaluate(EvaluationContext context) throws Exception {
        Comparable<Comparable> lv = (Comparable) left.evaluate(context);
        if (lv == null) {
            return null;
        }
        Comparable rv = (Comparable) right.evaluate(context);
        if (rv == null) {
            return null;
        }
        if (getExpressionSymbol().equals(">=") || getExpressionSymbol().equals(">")
            || getExpressionSymbol().equals("<") || getExpressionSymbol().equals("<=")) {
            Class<? extends Comparable> lc = lv.getClass();
            Class<? extends Comparable> rc = rv.getClass();
            if (lc == rc && lc == String.class) {
                // Compare String is illegal
                // first try to convert to double
                try {
                    Comparable lvC = Double.valueOf((String) (Comparable) lv);
                    Comparable rvC = Double.valueOf((String) rv);

                    return compare(lvC, rvC);
                } catch (Exception e) {
                    throw new RuntimeException("It's illegal to compare string by '>=', '>', '<', '<='. lv=" + lv + ", rv=" + rv, e);
                }
            }
        }
        return compare(lv, rv);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static int __compare(Comparable lv, Comparable rv, boolean convertStringExpressions) {
        Class<? extends Comparable> lc = lv.getClass();
        Class<? extends Comparable> rc = rv.getClass();
        // If the the objects are not of the same type,
        // try to convert up to allow the comparison.
        if (lc != rc) {
            try {
                if (lc == Boolean.class) {
                    if (convertStringExpressions && rc == String.class) {
                        lv = Boolean.valueOf((String) lv).booleanValue();
                    } else {
                        return -1;
                    }
                } else if (lc == Byte.class) {
                    if (rc == Short.class) {
                        lv = Short.valueOf(((Number) lv).shortValue());
                    } else if (rc == Integer.class) {
                        lv = Integer.valueOf(((Number) lv).intValue());
                    } else if (rc == Long.class) {
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Byte.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Short.class) {
                    if (rc == Integer.class) {
                        lv = Integer.valueOf(((Number) lv).intValue());
                    } else if (rc == Long.class) {
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Short.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Integer.class) {
                    if (rc == Long.class) {
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Integer.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Long.class) {
                    if (rc == Integer.class) {
                        rv = Long.valueOf(((Number) rv).longValue());
                    } else if (rc == Float.class) {
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Long.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Float.class) {
                    if (rc == Integer.class) {
                        rv = new Float(((Number) rv).floatValue());
                    } else if (rc == Long.class) {
                        rv = new Float(((Number) rv).floatValue());
                    } else if (rc == Double.class) {
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Float.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Double.class) {
                    if (rc == Integer.class) {
                        rv = new Double(((Number) rv).doubleValue());
                    } else if (rc == Long.class) {
                        rv = new Double(((Number) rv).doubleValue());
                    } else if (rc == Float.class) {
                        rv = new Float(((Number) rv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {
                        rv = Double.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (convertStringExpressions && lc == String.class) {
                    if (rc == Boolean.class) {
                        lv = Boolean.valueOf((String) lv);
                    } else if (rc == Byte.class) {
                        lv = Byte.valueOf((String) lv);
                    } else if (rc == Short.class) {
                        lv = Short.valueOf((String) lv);
                    } else if (rc == Integer.class) {
                        lv = Integer.valueOf((String) lv);
                    } else if (rc == Long.class) {
                        lv = Long.valueOf((String) lv);
                    } else if (rc == Float.class) {
                        lv = Float.valueOf((String) lv);
                    } else if (rc == Double.class) {
                        lv = Double.valueOf((String) lv);
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
        return lv.compareTo(rv);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Boolean compare(Comparable lv, Comparable rv) {
        return asBoolean(__compare(lv, rv, convertStringExpressions)) ? Boolean.TRUE : Boolean.FALSE;
    }

    protected abstract boolean asBoolean(int answer);

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

}
