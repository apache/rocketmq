package org.apache.rocketmq.auth.authentication.model;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.common.constant.CommonConstants;

public interface Subject {

    String toSubjectKey();

    SubjectType getSubjectType();

    default boolean isSubject(SubjectType subjectType) {
        return subjectType == this.getSubjectType();
    }

    @SuppressWarnings("unchecked")
    static <T extends Subject> T parseSubject(String subjectKey) {
        String type = StringUtils.substringBefore(subjectKey, CommonConstants.COLON);
        SubjectType subjectType = SubjectType.getByCode(type);
        if (subjectType == null) {
            return null;
        }
        switch (subjectType) {
            case USER:
                return (T) User.of(StringUtils.substringAfter(subjectKey, CommonConstants.COLON));
            default:
                return null;
        }
    }
}
