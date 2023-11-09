package org.apache.rocketmq.acl2.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class Environment {

    private List<String> sourceIps;

    public Environment() {
    }

    public Environment(String sourceIp) {
        this.sourceIps = new ArrayList<>();
        this.sourceIps.add(sourceIp);
    }

    public Environment(List<String> sourceIps) {
        this.sourceIps = sourceIps;
    }

    public boolean isMatch(Environment environment) {
        if (CollectionUtils.isEmpty(this.sourceIps)) {
            return true;
        }
        return false;
    }

    public List<String> getSourceIps() {
        return sourceIps;
    }

    public void setSourceIps(List<String> sourceIps) {
        this.sourceIps = sourceIps;
    }
}
