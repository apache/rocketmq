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
package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Test;

public class QueryMsgByUniqueKeySubCommandTest {

    @Test
    public void testExecute() throws SubCommandException {

        System.setProperty("rocketmq.namesrv.addr", "127.0.0.1:9876");

        QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();
        String[] args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

        args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-g producerGroupName", "-d "};
        commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

    }

}
