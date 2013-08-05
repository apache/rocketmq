package com.alibaba.rocketmq.common.filter.impl;

import static com.alibaba.rocketmq.common.filter.impl.Operator.LEFTPARENTHESIS;
import static com.alibaba.rocketmq.common.filter.impl.Operator.RIGHTPARENTHESIS;
import static com.alibaba.rocketmq.common.filter.impl.Operator.createOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;


/**
 * @auther lansheng.zj@taobao.com
 */
public class PolishExpr {

    /**
     * 拆分单词
     * 
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

            if ((97 <= chValue && chValue <= 122) || (65 <= chValue && chValue <= 90)
                    || (49 <= chValue && chValue <= 57) || 95 == chValue) {
                // 操作数

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
            }
            else if (40 == chValue || 41 == chValue) {
                // 括号

                if (Type.OPERATOR == preType) {
                    segments.add(createOperator(expression
                        .substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }
                else if (Type.OPERAND == preType) {
                    segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }

                preType = Type.PARENTHESIS;
                segments.add(createOperator((char) chValue + ""));
            }
            else if (38 == chValue || 124 == chValue) {
                // 操作符
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
            }
            else if (32 == chValue || 9 == chValue) {
                // 单词分隔符

                if (Type.OPERATOR == preType) {
                    segments.add(createOperator(expression
                        .substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }
                else if (Type.OPERAND == preType) {
                    segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
                    wordStartIndex = -1;
                    wordLen = 0;
                }
                preType = Type.SEPAERATOR;
            }
            else {
                // 非法字符
                throw new IllegalArgumentException("illegal expression, at index " + i + " " + (char) chValue);
            }

        }

        if (wordLen > 0) {
            segments.add(new Operand(expression.substring(wordStartIndex, wordStartIndex + wordLen)));
        }
        return segments;
    }


    /**
     * 将中缀表达式转换成逆波兰表达式
     * 
     * @param expression
     * @return
     */
    public static List<Op> reversePolish(String expression) {
        return reversePolish(participle(expression));
    }


    /**
     * 将中缀表达式转换成逆波兰表达式<br/>
     * Shunting-yard algorithm <br/>
     * http://en.wikipedia.org/wiki/Shunting_yard_algorithm
     * 
     * @param tokens
     * @return
     */
    public static List<Op> reversePolish(List<Op> tokens) {
        List<Op> segments = new ArrayList<Op>();
        Stack<Operator> operatorStack = new Stack<Operator>();

        for (int i = 0; i < tokens.size(); i++) {
            Op token = tokens.get(i);
            if (isOperand(token)) {
                // 操作数
                segments.add(token);
            }
            else if (isLeftParenthesis(token)) {
                // 左括号
                operatorStack.push((Operator) token);
            }
            else if (isRightParenthesis(token)) {
                // 右括号
                Operator opNew = null;
                while (!operatorStack.empty() && LEFTPARENTHESIS != (opNew = operatorStack.pop())) {
                    segments.add(opNew);
                }
                if (null == opNew || LEFTPARENTHESIS != opNew)
                    throw new IllegalArgumentException("mismatched parentheses");
            }
            else if (isOperator(token)) {
                // 操作符,暂不考虑结合性(左结合,右结合),支持的操作符都是左结合的
                Operator opNew = (Operator) token;
                if (!operatorStack.empty()) {
                    Operator opOld = operatorStack.peek();
                    if (opOld.isCompareable() && opNew.compare(opOld) != 1) {
                        segments.add(operatorStack.pop());
                    }
                }
                operatorStack.push(opNew);
            }
            else
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


    public static boolean isOperand(Op token) {
        return token instanceof Operand;
    }


    public static boolean isOperator(Op token) {
        return token instanceof Operator;
    }


    public static boolean isLeftParenthesis(Op token) {
        return token instanceof Operator && LEFTPARENTHESIS == (Operator) token;
    }


    public static boolean isRightParenthesis(Op token) {
        return token instanceof Operator && RIGHTPARENTHESIS == (Operator) token;
    }
}
