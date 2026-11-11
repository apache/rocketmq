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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.docs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link DocsDependencyChecker}.
 *
 * <p>Nothing in the Maven or Bazel build looks at the snippets in the documentation, which is how the
 * quick-start pages kept the Gradle {@code compile} configuration long after Gradle 7.0 removed it.
 * These tests pin down the rules on synthetic documents and then run the checker against the real
 * {@code docs} directory, so an edit that reintroduces a removed configuration, or that bumps the
 * dependency version on only one page, fails the build instead of reaching the quick start.
 */
public class DocsDependencyCheckerTest {
    private static final String ROCKETMQ_CLIENT = "org.apache.rocketmq:rocketmq-client";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final DocsDependencyChecker checker = new DocsDependencyChecker();

    @Test
    public void compileConfigurationIsReportedWithItsReplacement() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "Example_Simple.md", "gradle:", "``` groovy",
            "compile 'org.apache.rocketmq:rocketmq-client:5.5.0'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertEquals(problems.toString(), 1, problems.size());
        Assert.assertTrue(problems.toString(), problems.get(0).contains("the 'compile' configuration"));
        Assert.assertTrue(problems.toString(), problems.get(0).contains("use 'implementation'"));
    }

    @Test
    public void implementationConfigurationIsAccepted() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "Example_Simple.md", "``` groovy",
            "implementation 'org.apache.rocketmq:rocketmq-client:5.5.0'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertTrue(problems.toString(), problems.isEmpty());
    }

    @Test
    public void compileOnlyIsNotConfusedWithCompile() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "Example_Simple.md", "``` groovy",
            "compileOnly 'com.google.guava:guava:32.1.3-jre'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertTrue(problems.toString(), problems.isEmpty());
    }

    @Test
    public void removedConfigurationInKotlinDslIsReported() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "Example_Simple.md", "``` groovy", "dependencies {",
            "    testCompile(\"junit:junit:4.13.2\")", "}", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertEquals(problems.toString(), 1, problems.size());
        Assert.assertTrue(problems.toString(), problems.get(0).contains("use 'testImplementation'"));
    }

    @Test
    public void declarationsOutsideCodeBlocksAreIgnored() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "RocketMQ_Example.md",
            "Upgrade note: replace compile 'org.apache.rocketmq:rocketmq-client:5.5.0'.", "```",
            "implementation 'org.apache.rocketmq:rocketmq-client:5.5.0'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertTrue(problems.toString(), problems.isEmpty());
    }

    @Test
    public void coordinateWithoutVersionIsNotCompared() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        writeDocument(docsRoot, "Example_Simple.md", "``` groovy",
            "implementation 'org.apache.rocketmq:rocketmq-client'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertTrue(problems.toString(), problems.isEmpty());
    }

    @Test
    public void inconsistentRocketmqVersionsAreReported() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        Path englishDocs = Files.createDirectories(docsRoot.resolve("en"));
        writeDocument(englishDocs, "Example_Simple.md", "``` groovy",
            "implementation '" + ROCKETMQ_CLIENT + ":5.5.0'", "```");
        writeDocument(docsRoot, "RocketMQ_Example.md", "```",
            "implementation '" + ROCKETMQ_CLIENT + ":5.4.0'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertEquals(problems.toString(), 1, problems.size());
        Assert.assertTrue(problems.toString(), problems.get(0).contains("5.4.0"));
        Assert.assertTrue(problems.toString(), problems.get(0).contains("5.5.0"));
    }

    @Test
    public void consistentRocketmqVersionsAreAccepted() throws IOException {
        Path docsRoot = temporaryFolder.newFolder("docs").toPath();
        Path englishDocs = Files.createDirectories(docsRoot.resolve("en"));
        writeDocument(englishDocs, "Example_Simple.md", "``` groovy",
            "implementation '" + ROCKETMQ_CLIENT + ":5.5.0'",
            "implementation 'com.google.guava:guava:32.1.3-jre'", "```");
        writeDocument(docsRoot, "RocketMQ_Example.md", "```",
            "implementation '" + ROCKETMQ_CLIENT + ":5.5.0'", "```");

        List<String> problems = checker.check(docsRoot);

        Assert.assertTrue(problems.toString(), problems.isEmpty());
    }

    @Test
    public void quickStartDocumentsUseSupportedGradleConfigurations() throws IOException {
        List<String> problems = checker.check(repositoryDocsRoot());

        Assert.assertTrue("the quick-start snippets must stay valid: " + problems, problems.isEmpty());
    }

    /**
     * Writes one Markdown document below the given documentation root.
     */
    private Path writeDocument(Path docsRoot, String name, String... lines) throws IOException {
        StringBuilder content = new StringBuilder();
        for (String line : lines) {
            content.append(line).append('\n');
        }
        return Files.write(docsRoot.resolve(name), content.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Resolves the {@code docs} directory of the working copy. Surefire runs each module with the module
     * directory as working directory, so the repository root is found by walking upwards.
     */
    private Path repositoryDocsRoot() {
        Path candidate = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (candidate != null) {
            Path docsRoot = candidate.resolve("docs");
            if (Files.isRegularFile(docsRoot.resolve("en").resolve("Example_Simple.md"))) {
                return docsRoot;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("cannot locate the docs directory, working directory is "
            + System.getProperty("user.dir"));
    }
}
