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

package org.apache.rocketmq.common.filter.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import static org.apache.rocketmq.common.filter.impl.Operator.LEFTPARENTHESIS;
import static org.apache.rocketmq.common.filter.impl.Operator.RIGHTPARENTHESIS;
import static org.apache.rocketmq.common.filter.impl.Operator.createOperator;

public class PolishExpr {

    public static List<Op> reversePolish(String expression) {
        return reversePolish(participle(expression));
    }

    /**
     * Shunting-yard algorithm <br/>
     * http://en.wikipedia.org/wiki/Shunting_yard_algorithm
     *
     * @return the compute result of Shunting-yard algorithm
     */
    public static List<Op> reversePolish(List<Op> tokens) {
        List<Op> segments = new ArrayList<Op>();
        Stack<Operator> operatorStack = new Stack<Operator>();

        for (int i = 0; i < tokens.size(); i++) {
            Op token = tokens.get(i);
            if (isOperand(token)) {

                segments.add(token);
            } else if (isLeftParenthesis(token)) {

                operatorStack.push((Operator) token);
            } else if (isRightParenthesis(token)) {

                Operator opNew = null;
                while (!operatorStack.empty() && LEFTPARENTHESIS != (opNew = operatorStack.pop())) {
                    segments.add(opNew);
                }
                if (null == opNew || LEFTPARENTHESIS != opNew)
                    throw new IllegalArgumentException("mismatched parentheses");
            } else if (isOperator(token)) {

                Operator opNew = (Operator) token;
                if (!operatorStack.empty()) {
                    Operator opOld = operatorStack.peek();
                    if (opOld.isCompareable() && opNew.compare(opOld) != 1) {
                        segments.add(operatorStack.pop());
                    }
                }
                operatorStack.push(opNew);
            } else
                throw new IllegalArgumentException("illegal token " + token);
        }

        while (!operatorStack.empty()) {
            Operator operator = operatorStack.pop();
            if (LEFTPARENTHESIS == operator || RIGHTPARENTHESIS == operator)
                throw new IllegalArgumentException("mismatched parentheses " + operator);
            segments.add(operator);
        }

        return segments;
    }

    /**
     * @param expression
     * @return
     * @throws Exception
     */
    private static List<Op> participle(String expression) {
        List<Op> segments = new ArrayList<Op>();

        int size = expression.length();
        int wordStartIndex = -1;
        int wordLen = 0;
        Type preType = Type.NULL;

        for (int i = 0; i < size; i++) {
            int chValue = (int) expression.charAt(i);

            if (97 <= chValue && chValue <= 122 || 65 <= chValue && chValue <= 90
                || 49 <= chValue && chValue <= 57 || 95 == chValue) {

                if (Type.OPERATOR == preType || Type.SEPAERATOR == preType || Type.NULL == preType
                    || Type.PARENTHESIS == preType) {
                    if (Type.OPERATOR == preType) {
                        segments.add(createOperator(expression.substring(wordStartIndex, wordStartIndex
                            + wordLen)));
                    }
                    wordStartIndex = i;
                    wordLen = 0;
                }
                preType = Type.OPERAND;
                wordLen++;
            } else if (40 == chValue || 41 == chValue) {

                if (Type.OPERATOR == preType) {
                    segments.add(createOperator(expression
                        .substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                } else if (Type.OPERAND == preType) {
                    segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }

                preType = Type.PARENTHESIS;
                segments.add(createOperator((char) chValue + ""));
            } else if (38 == chValue || 124 == chValue) {

                if (Type.OPERAND == preType || Type.SEPAERATOR == preType || Type.PARENTHESIS == preType) {
                    if (Type.OPERAND == preType) {
                        segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex
                            + wordLen)));
                    }
                    wordStartIndex = i;
                    wordLen = 0;
                }
                preType = Type.OPERATOR;
                wordLen++;
            } else if (32 == chValue || 9 == chValue) {

                if (Type.OPERATOR == preType) {
                    segments.add(createOperator(expression
                        .substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                } else if (Type.OPERAND == preType) {
                    segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }
                preType = Type.SEPAERATOR;
            } else {

                throw new IllegalArgumentException("illegal expression, at index " + i + " " + (char) chValue);
            }

        }

        if (wordLen > 0) {
            segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
        }
        return segments;
    }

    public static boolean isOperand(Op token) {
        return token instanceof Operand;
    }

    public static boolean isLeftParenthesis(Op token) {
        return token instanceof Operator && LEFTPARENTHESIS == (Operator) token;
    }

    public static boolean isRightParenthesis(Op token) {
        return token instanceof Operator && RIGHTPARENTHESIS == (Operator) token;
    }

    public static boolean isOperator(Op token) {
        return token instanceof Operator;
    }
}
