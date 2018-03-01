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

package org.apache.rocketmq.logging.inner;


import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.junit.Assert;
import org.junit.Test;

public class MessageFormatterTest {

    @Test
    public void formatTest(){
        InnerLoggerFactory.FormattingTuple logging = InnerLoggerFactory.MessageFormatter.format("this is {},and {}", "logging", 6546);
        String message = logging.getMessage();
        Assert.assertTrue(message.contains("logging"));

        InnerLoggerFactory.FormattingTuple format = InnerLoggerFactory.MessageFormatter.format("cause exception {}", 143545, new RuntimeException());
        String message1 = format.getMessage();
        Throwable throwable = format.getThrowable();
        System.out.println(message1);
        Assert.assertTrue(throwable != null);
    }

}
