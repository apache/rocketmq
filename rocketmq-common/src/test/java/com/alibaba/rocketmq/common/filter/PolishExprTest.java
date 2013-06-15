package com.alibaba.rocketmq.common.filter;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.rocketmq.common.filter.impl.Op;
import com.alibaba.rocketmq.common.filter.impl.PolishExpr;


/**
 * @auther lansheng.zj@taobao.com
 */
public class PolishExprTest {

    private String expression = "tag1||(tag2&&tag3)&&tag4||tag5&&(tag6 && tag7)|| tag8    && tag9";
    private PolishExpr polishExpr;


    public void init() {
        polishExpr = new PolishExpr();
    }


    @Test
    public void testReversePolish() {
        List<Op> antiPolishExpression = polishExpr.reversePolish(expression);
        System.out.println(antiPolishExpression);
    }


    @Test
    public void testReversePolish_Performance() {
        // prepare
        for (int i = 0; i < 100000; i++) {
            polishExpr.reversePolish(expression);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            polishExpr.reversePolish(expression);
        }
        long cost = System.currentTimeMillis() - start;
        System.out.println(cost);
        // System.out.println(cost / 100000F);

        Assert.assertTrue(cost < 500);
    }

}
