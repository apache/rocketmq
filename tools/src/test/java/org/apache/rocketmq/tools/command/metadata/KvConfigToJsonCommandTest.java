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
package org.apache.rocketmq.tools.command.metadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

public class KvConfigToJsonCommandTest {
    private static final String BASE_PATH = System.getProperty("user.home") + File.separator + "store/config/";

    @Test
    public void testExecute() throws SubCommandException {
        {
            String[] cases = new String[] { "topics", "subscriptionGroups" };
            for (String c : cases) {
                RocksDBConfigToJsonCommand cmd = new RocksDBConfigToJsonCommand();
                Options options = ServerUtil.buildCommandlineOptions(new Options());
                String[] subargs = new String[] { "-p " + BASE_PATH + c, "-t " + c };
                final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                        cmd.buildCommandlineOptions(options), new DefaultParser());
                cmd.execute(commandLine, options, null);
                assertThat(commandLine.getOptionValue("p").trim()).isEqualTo(BASE_PATH + c);
                assertThat(commandLine.getOptionValue("t").trim()).isEqualTo(c);
            }
        }
        // invalid cases
        {
            String[][] cases = new String[][] {
                    { "-p " + BASE_PATH + "tmpPath", "-t topics" },
                    { "-p  ", "-t topics" },
                    { "-p " + BASE_PATH + "topics", "-t invalid_type" }
            };

            for (String[] c : cases) {
                RocksDBConfigToJsonCommand cmd = new RocksDBConfigToJsonCommand();
                Options options = ServerUtil.buildCommandlineOptions(new Options());
                final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), c,
                        cmd.buildCommandlineOptions(options), new DefaultParser());
                cmd.execute(commandLine, options, null);
            }
        }
    }
}
