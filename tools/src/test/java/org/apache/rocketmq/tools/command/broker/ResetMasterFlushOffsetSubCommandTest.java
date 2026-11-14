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
package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Assert;
import org.junit.Test;

public class ResetMasterFlushOffsetSubCommandTest {

    private Options buildOptions() {
        return new ResetMasterFlushOffsetSubCommand()
            .buildCommandlineOptions(ServerUtil.buildCommandlineOptions(new Options()));
    }

    @Test
    public void testMissingBrokerAddrIsRejected() {
        Assert.assertThrows(ParseException.class,
            () -> new DefaultParser().parse(buildOptions(), new String[] {"-o", "100"}));
    }

    @Test
    public void testMissingOffsetIsRejected() {
        Assert.assertThrows(ParseException.class,
            () -> new DefaultParser().parse(buildOptions(), new String[] {"-b", "127.0.0.1:10911"}));
    }

    @Test
    public void testParseBrokerAddrAndOffset() throws Exception {
        CommandLine commandLine = new DefaultParser().parse(buildOptions(),
            new String[] {"-b", "127.0.0.1:10911", "-o", "100"});

        Assert.assertEquals("127.0.0.1:10911", commandLine.getOptionValue('b'));
        Assert.assertEquals("100", commandLine.getOptionValue('o'));
    }
}
