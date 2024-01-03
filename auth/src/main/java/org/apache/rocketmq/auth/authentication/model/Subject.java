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
package org.apache.rocketmq.auth.authentication.model;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.common.constant.CommonConstants;

public interface Subject {

    String getSubjectKey();

    SubjectType getSubjectType();

    default boolean isSubject(SubjectType subjectType) {
        return subjectType == this.getSubjectType();
    }

    @SuppressWarnings("unchecked")
    static <T extends Subject> T of(String subjectKey) {
        String type = StringUtils.substringBefore(subjectKey, CommonConstants.COLON);
        SubjectType subjectType = SubjectType.getByName(type);
        if (subjectType == null) {
            return null;
        }
        if (subjectType == SubjectType.USER) {
            return (T) User.of(StringUtils.substringAfter(subjectKey, CommonConstants.COLON));
        }
        return null;
    }
}
