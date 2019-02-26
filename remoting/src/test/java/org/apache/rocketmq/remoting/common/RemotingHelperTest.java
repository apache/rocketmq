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

package org.apache.rocketmq.remoting.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingHelperTest {

    /**
     * This unit test case ensures that {@link RemotingHelper#exceptionExactDesc(Throwable)} could return the root cause of the exception.
     */
    @Test
    public void testExceptionExactDesc() {
        String exceptionExactDesc = null;

        try {
            generateNumberFormatException();
        } catch (Throwable t) {
            exceptionExactDesc = RemotingHelper.exceptionExactDesc(t);
        }

        assertThat(exceptionExactDesc).contains("RemotingHelperTest.generateNumberFormatException");
        assertThat(exceptionExactDesc).contains("NumberFormatException");
    }

    /**
     * This unit test case ensures that {@link RemotingHelper#exceptionSimpleDesc(Throwable)} could not return the root cause of the exception.
     */
    @Test
    public void testExceptionSimpleDesc() {
        String exceptionExactDesc = null;

        try {
            generateNumberFormatException();
        } catch (Throwable t) {
            exceptionExactDesc = RemotingHelper.exceptionSimpleDesc(t);
        }

        assertThat(exceptionExactDesc).doesNotContain("RemotingHelperTest.generateNumberFormatException");
        assertThat(exceptionExactDesc).contains("NumberFormatException");
    }


    /**
     * Generate a {@link NumberFormatException}.
     */
    private static void generateNumberFormatException() {
        String emptyStr = null;
        Long.parseLong(emptyStr);
    }
}
