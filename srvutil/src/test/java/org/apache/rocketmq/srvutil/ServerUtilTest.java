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

package org.apache.rocketmq.srvutil;

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class ServerUtilTest {

    @Test
    public void testBuildCommandlineOptions() {
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        assertThat(options.getOptions()).hasSize(2);

        Option helpOption = options.getOption("h");
        assertThat(helpOption).isNotNull();
        assertThat(helpOption.getLongOpt()).isEqualTo("help");
        assertThat(helpOption.isRequired()).isFalse();
        assertThat(helpOption.hasArg()).isFalse();

        Option namesrvAddrOption = options.getOption("n");
        assertThat(namesrvAddrOption).isNotNull();
        assertThat(namesrvAddrOption.getLongOpt()).isEqualTo("namesrvAddr");
        assertThat(namesrvAddrOption.isRequired()).isFalse();
        assertThat(namesrvAddrOption.hasArg()).isTrue();
    }

    @Test
    public void testCommandLine2Properties() {
        CommandLine commandLine = Mockito.mock(CommandLine.class);
        Option helpOption = new Option("h", "help", false, "Print help");
        Option namesrvAddrOption = new Option("n", "namesrvAddr", true, "Name server address list");

        when(commandLine.getOptions()).thenReturn(new Option[] {helpOption, namesrvAddrOption});
        when(commandLine.getOptionValue("help")).thenReturn(null);
        when(commandLine.getOptionValue("namesrvAddr")).thenReturn("127.0.0.1:9876");

        Properties properties = ServerUtil.commandLine2Properties(commandLine);

        assertThat(properties).hasSize(1);
        assertThat(properties.getProperty("namesrvAddr")).isEqualTo("127.0.0.1:9876");
        assertThat(properties.getProperty("help")).isNull();
    }

    @Test
    public void testCommandLine2PropertiesWhenOptionsNull() {
        CommandLine commandLine = Mockito.mock(CommandLine.class);

        when(commandLine.getOptions()).thenReturn(null);

        Properties properties = ServerUtil.commandLine2Properties(commandLine);

        assertThat(properties).isEmpty();
    }
}
