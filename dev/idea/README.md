<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Debug NameServer and Broker from IntelliJ IDEA

The shared `.run/Source_*.run.xml` configurations run the source modules directly.
They do not require a packaged distribution, a particular release version, a
machine-specific JDK name, or globally configured `ROCKETMQ_HOME`.

## Prepare the project once

1. Install JDK 17 and Maven 3.6.3 or newer. Import the repository's root `pom.xml` as a Maven
   project in IntelliJ IDEA. Set both the project SDK and Maven JDK to JDK 17.
   These configurations use the module/project SDK; they do not select an
   individually named SDK. Import the full Maven reactor rather than an
   individual module or the local Maven repository.
2. Initialize the protocol sources from the repository root:

   ```shell
   git submodule update --init --recursive
   ```

3. The shared configurations run Maven `compile` before launching each server.
   This generates protocol classes and compiles the selected module and its
   dependencies. A separate first build is optional; to prepare both servers
   from a terminal, run:

   ```shell
   mvn -pl "namesrv,broker" -am "-Dmaven.test.skip=true" compile
   ```

   Keep the quotes around the module list and the `-D` argument, including in
   Windows PowerShell, which can otherwise split dotted property arguments.
   This uses your normal Maven settings. No repository mirror or absolute path
   needs to be committed. `rocketmq-proto/pom.xml` explicitly registers both
   generated source directories through `build-helper-maven-plugin`, so Maven
   importers do not need to discover the nested layout by scanning:

   - `target/generated-sources/protobuf/java`
   - `target/generated-sources/protobuf/grpc-java`

   Reload Maven after updating from an older version of these configurations
   so IDEA imports the new source-root declarations. Do not mark all of `target`
   as a source root or commit the generated Java files.
4. In **Run > Edit Configurations**, check the server's **Before launch** task
   is **Run Maven Goal**, not IDEA's **Build**. The checked-in task runs `compile`
   with tests, Checkstyle and SpotBugs skipped for local debugging. Normal Maven
   builds retain their checks. Both the project SDK and Maven runner JDK must
   be JDK 17.

The first run downloads Maven dependencies as needed and is slower than later
runs. JDK/Maven installation, Maven import and submodule initialization are
still prerequisites. Maven compilation uses the POM's Java 8 source/target
settings, so these launches do not require changing IDEA's `--release` option.
If you separately use IDEA's native **Build Project** with JDK 17, disable
**Use '--release' option for cross-compilation (Java 9 and later)**: `--release 8`
hides the internal `sun.misc.Unsafe` API used by this source. The shared
configurations deliberately do not invoke that native builder.

## Start and debug

Ensure another NameServer or Broker is not using the ports below.

1. Select **Source - NameServer** in the run configuration selector and run it.
   Wait for `The Name Server boot success.`
2. Select **Source - Broker** and run it without stopping NameServer.
   Wait for `The broker[broker-source, 127.0.0.1:10911] boot success.`
   Before starting clients, also wait for `Registering current broker to name
   server completed. TargetHost=127.0.0.1:9876` in
   `devenv/idea/home/logs/rocketmqlogs/broker.log`. Port readiness and the boot
   message alone do not guarantee that the first NameServer registration has
   completed; the scheduled registration can take another ten seconds.
3. Point your producer and consumer at `127.0.0.1:9876`. Set the address explicitly
   with `setNamesrvAddr("127.0.0.1:9876")`, or set `NAMESRV_ADDR=127.0.0.1:9876`
   in each client's own run configuration. Environment settings are not shared
   between separate IDEA run configurations.

Use **Debug** instead of **Run** to stop at breakpoints in `NamesrvStartup`,
`BrokerStartup`, or the message processors. Stop the client applications, then
Broker, then NameServer using their corresponding IDEA stop buttons. No
background startup script is involved.

## Paths and resource limits

The working directory is the repository root. Paths containing spaces are quoted
in both run configurations.

| Item | Location or value |
| --- | --- |
| NameServer client port | `127.0.0.1:9876` |
| Broker client port | `127.0.0.1:10911` |
| Broker fast client port | `127.0.0.1:10909` |
| HA replication port | `10912`, with the upstream HA listener's binding behavior |
| NameServer maximum Java heap | 256 MiB |
| Broker maximum Java heap | 512 MiB |
| Maximum direct memory per process | 256 MiB |
| CommitLog segment size | 64 MiB |
| Message data | `devenv/idea/store/` |
| Logs | `devenv/idea/home/logs/rocketmqlogs/` |
| NameServer persisted metadata | `devenv/idea/home/namesrv/` |

These are local learning defaults. Total process memory also includes thread
stacks, mapped files and other JVM overhead. Timer-wheel messages and compaction
are disabled to reduce the initial footprint; enable them explicitly when
debugging those features. Async flushing and automatic topic creation are
enabled. Proxy, authentication and replication peers are not started.

The existing `devenv` ignore rule excludes runtime data. Stopping or restarting
the processes keeps that data, and Maven `clean` does not remove it. Do not share
the store directory with another running Broker.

## Troubleshooting

- **Configuration is missing:** open the repository root as the IDEA project
  and use **Run > Edit Configurations** to find **Source - NameServer** and
  **Source - Broker**.
- **Main class not found:** check that Maven import and compilation completed.
  A failed rebuild can remove the old output without generating replacement
  classes. Do not skip a failed **Before launch: Run Maven Goal** step.
- **Unsafe is not found:** make sure the configuration runs the Maven goal and
  has no additional native **Build** task. The `--release` setting above applies
  only when choosing to use IDEA's native compiler separately.
- **Protocol package is not found:** check the two generated roots above and
  reload the updated POM, and check that the Maven pre-launch task succeeds. Check the project
  SDK, module SDKs and Maven runner JDK are all JDK 17; the run configurations
  inherit those settings rather than installing or selecting a named SDK.
- **GPG plugin cannot be resolved:** the root POM declares the signing plugin
  for releases. Local source debugging does not require signing. With the same
  Maven settings used by IDEA, resolve its help goal with
  `mvn -N org.apache.maven.plugins:maven-gpg-plugin:1.6:help` and reload Maven.
  Do not activate the release profile or remove signing configuration to work
  around a missing plugin download.
- **Port already in use:** stop the other instance, or adjust both server
  configuration files and your clients to use a consistent set of free ports.
  Changing the Broker's client port also changes its fast client port
  (`listenPort - 2`).
- **No route for a topic:** check Broker's successful startup and NameServer
  registration, the client's address, and that producer and consumer use the
  same topic. These server configurations do not create client-specific topics
  in advance.

The configurations are intended for JDK 17. Other JDK versions and operating
systems require separate verification.
