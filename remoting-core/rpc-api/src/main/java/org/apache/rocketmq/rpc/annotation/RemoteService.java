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

package org.apache.rocketmq.rpc.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RemoteService {
    String name();

    /**
     * The API version (NOT the product version), composed as a dot delimited
     * string with major, minor, and patch level components.
     * <pre>
     * - Major: Incremented for backward incompatible changes. An example would
     * be changes to the number or disposition of method arguments.
     * - Minor: Incremented for backward compatible changes. An example would
     * be the addition of a new (optional) method.
     * - Patch: Incremented for bug fixes. The patch level should be increased
     * for every edit that doesn't result in a change to major/minor.
     * </pre>
     * See the Semantic Versioning Specification (SemVer) http://semver.org.
     *
     * @return the string version presentation
     */
    String version() default "1.0.0";

    String description() default "";
}
