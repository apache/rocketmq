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

        System.setProperty("rocketmq.namesrv.addr", "10.58.84.247:9876");

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
