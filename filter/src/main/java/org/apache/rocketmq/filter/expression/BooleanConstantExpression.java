package org.apache.rocketmq.filter.expression;

/**
 * Created by bruce in 2020/3/31 23:52
 */
public class BooleanConstantExpression extends ConstantExpression implements BooleanExpression {

    public static final BooleanConstantExpression NULL = new BooleanConstantExpression(null);
    public static final BooleanConstantExpression TRUE = new BooleanConstantExpression(Boolean.TRUE);
    public static final BooleanConstantExpression FALSE = new BooleanConstantExpression(Boolean.FALSE);

    public BooleanConstantExpression(Object value) {
        super(value);
    }

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }
}
