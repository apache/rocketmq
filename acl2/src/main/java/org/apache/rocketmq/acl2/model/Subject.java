package org.apache.rocketmq.acl2.model;

import org.apache.rocketmq.acl2.enums.SubjectType;

public interface Subject {

    String getSubjectKey();

    SubjectType subjectType();

    default boolean isSubject(SubjectType subjectType) {
        return subjectType == this.subjectType();
    }
}
