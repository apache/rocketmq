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
package org.apache.rocketmq.tools.command;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

public class MQHealthCheckStartupTest {
    @Test
    public void testInvalidTargetCombinationReturnsUsageExitCode() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = MQHealthCheckStartup.main0(
            new String[] {"-b", "127.0.0.1:10911", "-s"}, null,
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(error));

        Assert.assertEquals(MQHealthCheckStartup.EXIT_USAGE_OR_ERROR, exitCode);
        Assert.assertTrue(error.toString().contains("cannot be used together"));
    }

    @Test
    public void testInvalidTimeoutReturnsUsageExitCode() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = MQHealthCheckStartup.main0(
            new String[] {"-s", "-t", "0"}, null,
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(error));

        Assert.assertEquals(MQHealthCheckStartup.EXIT_USAGE_OR_ERROR, exitCode);
        Assert.assertTrue(error.toString().contains("timeoutMillis"));
    }

    @Test
    public void testInvalidFormatReturnsUsageExitCodeBeforeNetworkAccess() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = MQHealthCheckStartup.main0(
            new String[] {"-s", "-f", "yaml"}, null,
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(error));

        Assert.assertEquals(MQHealthCheckStartup.EXIT_USAGE_OR_ERROR, exitCode);
        Assert.assertTrue(error.toString().contains("format must be text or json"));
    }

    @Test
    public void testExitCodeConstantsAreStableForContainerHealthChecks() {
        Assert.assertEquals(0, MQHealthCheckStartup.EXIT_HEALTHY);
        Assert.assertEquals(1, MQHealthCheckStartup.EXIT_UNHEALTHY);
        Assert.assertEquals(2, MQHealthCheckStartup.EXIT_USAGE_OR_ERROR);
    }
}
