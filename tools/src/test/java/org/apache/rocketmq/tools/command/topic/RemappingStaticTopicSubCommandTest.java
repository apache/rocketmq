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
package org.apache.rocketmq.tools.command.topic;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class RemappingStaticTopicSubCommandTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testFromFileModeIsSelectedByTheMapFileOption() throws Exception {
        // the documented from-file invocation: the mapping detail has to come from the named file,
        // never from a mapping recomputed out of the cluster's current state
        File mapFile = writeMappingFile();
        CommandLine commandLine = parse(new String[] {"-t", "unit-test-topic", "-b", "brokerA",
            "-mf", mapFile.getAbsolutePath()});

        TopicRemappingDetailWrapper loaded = RemappingStaticTopicSubCommand.loadMappingDetail(commandLine);

        assertThat(loaded.getEpoch()).isEqualTo(4L);
        assertThat(loaded.getBrokerConfigMap()).containsKey("brokerA");
    }

    @Test
    public void testFromFileModeParsesWithoutTargetBrokers() throws Exception {
        // the file carries the target brokers, so neither -b nor -c is needed for this mode. The
        // plain parser is used on purpose: the mqadmin layer turns a parse error into exit(1).
        File mapFile = writeMappingFile();
        RemappingStaticTopicSubCommand command = new RemappingStaticTopicSubCommand();
        Options options = command.buildCommandlineOptions(ServerUtil.buildCommandlineOptions(new Options()));

        CommandLine commandLine = new DefaultParser().parse(options,
            new String[] {"-t", "unit-test-topic", "-mf", mapFile.getAbsolutePath()});

        assertThat(commandLine.hasOption("mf")).isTrue();
    }

    private CommandLine parse(String[] args) throws Exception {
        RemappingStaticTopicSubCommand command = new RemappingStaticTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        return ServerUtil.parseCmdLine("mqadmin " + command.commandName(), args,
            command.buildCommandlineOptions(options), new DefaultParser());
    }

    private File writeMappingFile() throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        TopicConfigAndQueueMapping mapping = new TopicConfigAndQueueMapping();
        mapping.setTopicName("unit-test-topic");
        brokerConfigMap.put("brokerA", mapping);
        TopicRemappingDetailWrapper wrapper = new TopicRemappingDetailWrapper("unit-test-topic",
            TopicRemappingDetailWrapper.TYPE_REMAPPING, 4L, brokerConfigMap, new HashSet<>(), new HashSet<>());

        File mapFile = tempFolder.newFile("static-topic-mapping.json");
        Files.write(mapFile.toPath(), wrapper.encode());
        return mapFile;
    }
}
