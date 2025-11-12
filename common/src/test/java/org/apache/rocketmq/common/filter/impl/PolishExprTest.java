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

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;


class PolishExprTest {

    @Test
    void testIsLeftOrRightParenthesis() {

        Op leftParenthesis = new Op("(") { } ;
        Op rightParenthesis = new Op(")") { } ;

        assertFalse(PolishExpr.isLeftParenthesis(null));
        assertFalse(PolishExpr.isRightParenthesis(null));

        assertFalse(PolishExpr.isLeftParenthesis(leftParenthesis));
        assertFalse(PolishExpr.isLeftParenthesis(rightParenthesis));
        assertFalse(PolishExpr.isRightParenthesis(leftParenthesis));
        assertFalse(PolishExpr.isRightParenthesis(rightParenthesis));

        leftParenthesis = new Operand("(") { } ;
        rightParenthesis = new Operand(")") { } ;

        assertFalse(PolishExpr.isLeftParenthesis(leftParenthesis));
        assertFalse(PolishExpr.isLeftParenthesis(rightParenthesis));
        assertFalse(PolishExpr.isRightParenthesis(leftParenthesis));
        assertFalse(PolishExpr.isRightParenthesis(rightParenthesis));

        leftParenthesis = Operator.createOperator("(");
        rightParenthesis = Operator.createOperator(")");

        assertTrue(PolishExpr.isLeftParenthesis(leftParenthesis));
        assertFalse(PolishExpr.isLeftParenthesis(rightParenthesis));
        assertFalse(PolishExpr.isRightParenthesis(leftParenthesis));
        assertTrue(PolishExpr.isRightParenthesis(rightParenthesis));

    }

}