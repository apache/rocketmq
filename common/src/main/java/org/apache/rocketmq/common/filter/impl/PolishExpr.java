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
        List<Op> segments = new ArrayList<>();
        Stack<Operator> operatorStack = new Stack<>();

        for (Op token : tokens) {
            if (isOperand(token)) {
                segments.add(token);
            } else if (isLeftParenthesis(token)) {
                operatorStack.push((Operator) token);
            } else if (isRightParenthesis(token)) {
                handleRightParenthesis(operatorStack, segments);
            } else if (isOperator(token)) {
                handleOperator((Operator) token, operatorStack, segments);
            } else {
                throw new IllegalArgumentException("illegal token " + token);
            }
        }

        while (!operatorStack.empty()) {
            Operator operator = operatorStack.pop();
            if (LEFTPARENTHESIS == operator || RIGHTPARENTHESIS == operator) {
                throw new IllegalArgumentException("mismatched parentheses " + operator);
            }
            segments.add(operator);
        }

        return segments;
    }

    private static void handleRightParenthesis(Stack<Operator> operatorStack, List<Op> segments) {
        Operator opNew = null;
        while (!operatorStack.empty() && LEFTPARENTHESIS != (opNew = operatorStack.pop())) {
            segments.add(opNew);
        }
        if (null == opNew || LEFTPARENTHESIS != opNew) {
            throw new IllegalArgumentException("mismatched parentheses");
        }
    }

    private static void handleOperator(Operator opNew, Stack<Operator> operatorStack, List<Op> segments) {
        while (!operatorStack.empty()) {
            Operator opOld = operatorStack.peek();
            if (opOld.isCompareable() && opNew.compare(opOld) != 1) {
                segments.add(operatorStack.pop());
            } else {
                break;
            }
        }
        operatorStack.push(opNew);
    }

    private static List<Op> participle(String expression) {
        List<Op> segments = new ArrayList<>();
        int size = expression.length();
        int wordStartIndex = -1;
        int wordLen = 0;
        Type preType = Type.NULL;

        for (int i = 0; i < size; i++) {
            int chValue = (int) expression.charAt(i);
            Type currentType = getType(chValue);

            if (currentType == Type.OPERAND && (preType == Type.OPERATOR || preType == Type.SEPAERATOR || preType == Type.NULL || preType == Type.PARENTHESIS)) {
                if (preType == Type.OPERATOR) {
                    addToken(segments, expression, wordStartIndex, wordLen);
                }
                wordStartIndex = i;
                wordLen = 0;
            }

            if (currentType != Type.OPERAND) {
                addToken(segments, expression, wordStartIndex, wordLen);
                wordStartIndex = -1;
                wordLen = 0;
            }

            preType = currentType;
            if (currentType == Type.OPERAND) {
                wordLen++;
            }
        }

        if (wordLen > 0) {
            segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
        }
        return segments;
    }

    private static void addToken(List<Op> segments, String expression, int wordStartIndex, int wordLen) {
        if (wordStartIndex >= 0) {
            if (wordLen > 0) {
                segments.add(createOperator(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
            }
            wordStartIndex = -1;
            wordLen = 0;
        }
    }

    private static Type getType(int chValue) {
        if (Character.isLetterOrDigit(chValue) || chValue == '_') {
            return Type.OPERAND;
        } else if (chValue == '(') {
            return Type.PARENTHESIS;
        } else if (chValue == ')') {
            return Type.PARENTHESIS;
        } else if (chValue == '&' || chValue == '|') {
            return Type.OPERATOR;
        } else if (chValue == ' ' || chValue == '\t') {
            return Type.SEPAERATOR;
        } else {
            throw new IllegalArgumentException("illegal expression, at index " + chValue);
        }
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
