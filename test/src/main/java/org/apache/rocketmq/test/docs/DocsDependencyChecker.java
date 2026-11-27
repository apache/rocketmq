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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates the build snippets that the documentation asks readers to copy.
 *
 * <p>RocketMQ is built with Maven and Bazel, so no build job examines the Markdown files under
 * {@code docs/}. That is how the quick-start pages kept advertising the Gradle {@code compile}
 * configuration long after Gradle 7.0 removed it: the snippet still looks plausible, and only a user
 * who pastes it into an empty project sees "Could not find method compile()". This checker turns that
 * silent drift into a build failure by inspecting every dependency declaration that appears inside a
 * fenced code block and reporting:
 *
 * <ul>
 * <li>configurations that Gradle 7.0 removed, together with the configuration to write instead, and
 * </li>
 * <li>documents that disagree about the version of the same {@code org.apache.rocketmq} artifact, so a
 * version bump has to be applied to every page instead of one page at a time.</li>
 * </ul>
 *
 * <p>The class deliberately depends on nothing but the JDK, so the same code can be called from a unit
 * test, from a documentation job or from a developer shell.
 */
public class DocsDependencyChecker {
    /** Directory inspected by {@link #main(String[])} when the working directory is the repository root. */
    private static final String DEFAULT_DOCS_DIRECTORY = "docs";

    /** Group whose artifacts must not be documented with conflicting versions. */
    private static final String ROCKETMQ_GROUP = "org.apache.rocketmq";

    /** Gradle configurations removed in 7.0, mapped to the configuration that replaces them. */
    private static final Map<String, String> REMOVED_CONFIGURATIONS = removedConfigurations();

    /**
     * Dependency declarations understood by this checker, in Groovy form
     * ({@code implementation 'group:artifact:version'}) and Kotlin form
     * ({@code implementation("group:artifact:version")}). Only a line that consists of nothing but the
     * declaration matches, so prose and ordinary Java snippets are never reported.
     */
    private static final Pattern DECLARATION = Pattern.compile(
        "^\\s*(compile|compileOnly|implementation|api|runtime|runtimeOnly"
            + "|testCompile|testCompileOnly|testImplementation|testRuntime|testRuntimeOnly"
            + "|annotationProcessor|testAnnotationProcessor)"
            + "\\s*\\(?\\s*['\"]([^'\"]+)['\"]\\s*\\)?\\s*$");

    /**
     * Checks every Markdown document below the given documentation root.
     *
     * @param docsRoot directory holding the documentation, usually {@code <repository>/docs}
     * @return one message per problem, ordered by document and line, empty when the snippets are valid
     * @throws IOException when the documentation directory cannot be read
     */
    public List<String> check(Path docsRoot) throws IOException {
        List<String> problems = new ArrayList<String>();
        Map<String, String> knownVersions = new LinkedHashMap<String, String>();
        Map<String, DependencyDeclaration> firstDeclarations = new LinkedHashMap<String, DependencyDeclaration>();
        for (Path document : markdownDocuments(docsRoot)) {
            for (DependencyDeclaration declaration : parse(document, docsRoot)) {
                reportRemovedConfiguration(declaration, problems);
                reportVersionConflict(declaration, knownVersions, firstDeclarations, problems);
            }
        }
        return problems;
    }

    /**
     * Reports the configurations that Gradle 7.0 removed. The replacement is part of the message
     * because the original failure ("Could not find method compile()") does not tell the reader what
     * to write instead.
     */
    private void reportRemovedConfiguration(DependencyDeclaration declaration, List<String> problems) {
        String replacement = REMOVED_CONFIGURATIONS.get(declaration.getConfiguration());
        if (replacement != null) {
            problems.add(declaration.getSourceFile() + ":" + declaration.getLineNumber()
                + ": Gradle 7.0 removed the '" + declaration.getConfiguration() + "' configuration, use '"
                + replacement + "' instead: " + declaration.getCoordinate());
        }
    }

    /**
     * Reports two documents that pin different versions of the same RocketMQ artifact. Several pages
     * document the same dependency, so a version bump that misses one of them leaves an inconsistent
     * quick start behind.
     */
    private void reportVersionConflict(DependencyDeclaration declaration, Map<String, String> knownVersions,
        Map<String, DependencyDeclaration> firstDeclarations, List<String> problems) {
        if (!ROCKETMQ_GROUP.equals(declaration.getGroupId()) || !declaration.hasVersion()) {
            return;
        }
        String key = declaration.getGroupArtifact();
        String knownVersion = knownVersions.get(key);
        if (knownVersion == null) {
            knownVersions.put(key, declaration.getVersion());
            firstDeclarations.put(key, declaration);
            return;
        }
        if (!knownVersion.equals(declaration.getVersion())) {
            DependencyDeclaration first = firstDeclarations.get(key);
            problems.add(declaration.getSourceFile() + ":" + declaration.getLineNumber()
                + ": version " + declaration.getVersion() + " of " + key + " does not match version "
                + knownVersion + " declared at " + first.getSourceFile() + ":" + first.getLineNumber());
        }
    }

    /**
     * Returns every Markdown document below the documentation root, sorted so that the problems of
     * {@link #check(Path)} are reported in a stable order.
     */
    private List<Path> markdownDocuments(Path docsRoot) throws IOException {
        if (!Files.isDirectory(docsRoot)) {
            throw new IOException("documentation directory not found: " + docsRoot);
        }
        List<Path> documents = new ArrayList<Path>();
        try (Stream<Path> walk = Files.walk(docsRoot)) {
            documents.addAll(walk.filter(path -> Files.isRegularFile(path))
                .filter(path -> isMarkdown(path)).collect(Collectors.toList()));
        }
        Collections.sort(documents);
        return documents;
    }

    private static boolean isMarkdown(Path document) {
        String name = document.getFileName().toString().toLowerCase(Locale.ENGLISH);
        return name.endsWith(".md");
    }

    /**
     * Parses the dependency declarations of one document. Only lines inside fenced code blocks are
     * considered, because that is where copy-and-paste snippets live; a block is opened and closed by a
     * line that starts with three backticks or three tildes.
     */
    List<DependencyDeclaration> parse(Path document, Path docsRoot) throws IOException {
        List<DependencyDeclaration> declarations = new ArrayList<DependencyDeclaration>();
        String relativePath = docsRoot.relativize(document).toString().replace('\\', '/');
        boolean inCodeBlock = false;
        String fence = "";
        List<String> lines = readLines(document);
        for (int index = 0; index < lines.size(); index++) {
            String trimmed = lines.get(index).trim();
            if (trimmed.startsWith("```") || trimmed.startsWith("~~~")) {
                String marker = trimmed.startsWith("~~~") ? "~~~" : "```";
                if (!inCodeBlock) {
                    inCodeBlock = true;
                    fence = marker;
                } else if (marker.equals(fence)) {
                    inCodeBlock = false;
                    fence = "";
                }
                continue;
            }
            if (!inCodeBlock) {
                continue;
            }
            Matcher matcher = DECLARATION.matcher(lines.get(index));
            if (matcher.matches()) {
                declarations.add(new DependencyDeclaration(relativePath, index + 1, matcher.group(1), matcher.group(2)));
            }
        }
        return declarations;
    }

    private static List<String> readLines(Path document) throws IOException {
        byte[] bytes = Files.readAllBytes(document);
        String content = new String(bytes, StandardCharsets.UTF_8).replace("\r\n", "\n");
        return Arrays.asList(content.split("\n", -1));
    }

    private static Map<String, String> removedConfigurations() {
        Map<String, String> removed = new LinkedHashMap<String, String>();
        removed.put("compile", "implementation");
        removed.put("runtime", "runtimeOnly");
        removed.put("testCompile", "testImplementation");
        removed.put("testRuntime", "testRuntimeOnly");
        return removed;
    }

    /**
     * Runs the checker against the {@code docs} directory of the working copy, so a documentation job
     * can use it without any build tool integration.
     *
     * @param args optional documentation directory, defaults to {@code docs}
     * @throws IOException when the documentation directory cannot be read
     */
    public static void main(String[] args) throws IOException {
        Path docsRoot = args.length > 0 ? Paths.get(args[0]) : Paths.get(DEFAULT_DOCS_DIRECTORY);
        List<String> problems = new DocsDependencyChecker().check(docsRoot);
        for (String problem : problems) {
            System.err.println(problem);
        }
        if (!problems.isEmpty()) {
            System.err.printf("%d documentation dependency snippet(s) must be fixed%n", problems.size());
            System.exit(1);
        }
        System.out.printf("checked dependency snippets in %s: %d problem(s)%n", docsRoot, problems.size());
    }
}
