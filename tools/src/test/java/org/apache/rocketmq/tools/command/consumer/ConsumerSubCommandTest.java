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
package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerSubCommandTest {

    @Test
    public void testParseClientIdWithConsumerGroup() throws Exception {
        ConsumerSubCommand cmd = new ConsumerSubCommand();
        Options options = cmd.buildCommandlineOptions(ServerUtil.buildCommandlineOptions(new Options()));

        CommandLine commandLine = new DefaultParser().parse(options,
            new String[] {"-g", "default-group", "-i", "cid_one", "-s"});

        Assert.assertEquals("default-group", commandLine.getOptionValue('g'));
        Assert.assertEquals("cid_one", commandLine.getOptionValue('i'));
        Assert.assertTrue(commandLine.hasOption('s'));
    }
}
