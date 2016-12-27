/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.filter.impl.Op;
import com.alibaba.rocketmq.common.filter.impl.PolishExpr;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;


/**
 * @author lansheng.zj
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
