package com.alibaba.rocketmq.common.filter.impl;

/**
 * @auther lansheng.zj@taobao.com
 */
public class Operator extends Op {

    public static final Operator LEFTPARENTHESIS = new Operator("(", 30, false);
    public static final Operator RIGHTPARENTHESIS = new Operator(")", 30, false);
    public static final Operator AND = new Operator("&&", 20, true);
    public static final Operator OR = new Operator("||", 15, true);

    private int priority;
    private boolean compareable;


    private Operator(String symbol, int priority, boolean compareable) {
        super(symbol);
        this.priority = priority;
        this.compareable = compareable;
    }


    public int getPriority() {
        return priority;
    }


    public boolean isCompareable() {
        return compareable;
    }


    // -1 小于; 0 等于; 1大于
    public int compare(Operator operator) {
        if (this.priority > operator.priority)
            return 1;
        else if (this.priority == operator.priority)
            return 0;
        else
            return -1;
    }


    public boolean isSpecifiedOp(String operator) {
        return this.getSymbol().equals(operator);
    }


    public static Operator createOperator(String operator) {
        if (LEFTPARENTHESIS.getSymbol().equals(operator))
            return LEFTPARENTHESIS;
        else if (RIGHTPARENTHESIS.getSymbol().equals(operator))
            return RIGHTPARENTHESIS;
        else if (AND.getSymbol().equals(operator))
            return AND;
        else if (OR.getSymbol().equals(operator))
            return OR;
        else
            throw new IllegalArgumentException("unsupport operator " + operator);
    }
}
