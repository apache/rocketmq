package org.apache.rocketmq.tools.command.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author wb-zh378814
 * @Date 2021/12/16 17:39
 */
public class StatsAllSubCommandTest {

    @Test
    public void testExecute() {
        StatsAllSubCommand statsAllSubCommand = new StatsAllSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-n 127.0.0.1:9876"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + statsAllSubCommand.commandName(), subargs, statsAllSubCommand.buildCommandlineOptions(options), new PosixParser());
        assertThat(commandLine.getOptionValue("n").trim()).isEqualTo("127.0.0.1:9876");
    }

}
