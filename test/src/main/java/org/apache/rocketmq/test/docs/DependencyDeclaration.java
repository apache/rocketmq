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

/**
 * A single build dependency declaration parsed from a fenced code block of a Markdown document.
 *
 * <p>The quick-start pages are meant to be copy-and-paste ready, so a stale entry is not a cosmetic
 * problem: the reader pastes it into an empty project and the build fails before the first RocketMQ
 * class is loaded. Keeping the parsed line as a small value object lets {@link DocsDependencyChecker}
 * report the exact document, line and configuration that need to be corrected.
 */
public class DependencyDeclaration {
    private final String sourceFile;
    private final int lineNumber;
    private final String configuration;
    private final String coordinate;
    private final String groupId;
    private final String artifactId;
    private final String version;

    public DependencyDeclaration(String sourceFile, int lineNumber, String configuration, String coordinate) {
        this.sourceFile = sourceFile;
        this.lineNumber = lineNumber;
        this.configuration = configuration;
        this.coordinate = coordinate;
        String[] parts = coordinate.split(":");
        this.groupId = parts[0].trim();
        this.artifactId = parts.length > 1 ? parts[1].trim() : "";
        this.version = parts.length > 2 ? parts[2].trim() : "";
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public String getConfiguration() {
        return configuration;
    }

    public String getCoordinate() {
        return coordinate;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    /**
     * Returns the {@code group:artifact} key used to compare the versions of one dependency across
     * documents, for example {@code org.apache.rocketmq:rocketmq-client}.
     */
    public String getGroupArtifact() {
        return groupId + ":" + artifactId;
    }

    /**
     * Returns {@code true} when the coordinate pins a version. Coordinates without a version cannot
     * be compared with each other, so they are ignored by the version consistency rule.
     */
    public boolean hasVersion() {
        return !version.isEmpty();
    }

    @Override
    public String toString() {
        return sourceFile + ":" + lineNumber + " " + configuration + " '" + coordinate + "'";
    }
}
