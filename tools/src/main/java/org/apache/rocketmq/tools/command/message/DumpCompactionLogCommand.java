package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DumpCompactionLogCommand implements SubCommand {
    @Override
    public String commandDesc() {
        return "parse compaction log to message";
    }

    @Override
    public String commandName() {
        return "dumpCompactionLog";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("f", "file", true, "to dump file name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
            throws SubCommandException, IOException {
        if (commandLine.hasOption("f")) {
            String fileName = commandLine.getOptionValue("f");
            if (!Files.exists(Paths.get(fileName))) {
                throw new SubCommandException("file " + fileName + " not exist.");
            }

            if (Files.isDirectory(Paths.get(fileName))) {
                throw new SubCommandException("file " + fileName + " is a directory.");
            }

            long fileSize = Files.size(Paths.get(fileName));
            FileChannel fileChannel = new RandomAccessFile(fileName, "rw").getChannel();
            ByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            Files.readAllBytes();
            MessageDecoder.decode();
        } else {
            System.out.println("miss dump log file name");
        }


    }
}
