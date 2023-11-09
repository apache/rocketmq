package org.apache.rocketmq.remoting.protocol.body;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AclInfo {

    private String subject;

    private List<PolicyInfo> policies;

    public static AclInfo of(String subject, List<String> resources, List<String> actions,
        List<String> sourceIps,
        String decision) {
        AclInfo aclInfo = new AclInfo();
        aclInfo.setSubject(subject);
        PolicyInfo policyInfo = PolicyInfo.of(resources, actions, sourceIps, decision);
        aclInfo.setPolicies(Collections.singletonList(policyInfo));
        return aclInfo;
    }

    public static class PolicyInfo {

        private String policyType;

        private List<PolicyEntryInfo> entries;

        public static PolicyInfo of(List<String> resources, List<String> actions,
            List<String> sourceIps, String decision) {
            PolicyInfo policyInfo = new PolicyInfo();
            List<PolicyEntryInfo> entries = resources.stream()
                .map(resource -> PolicyEntryInfo.of(resource, actions, sourceIps, decision))
                .collect(Collectors.toList());
            policyInfo.setEntries(entries);
            return policyInfo;
        }

        public String getPolicyType() {
            return policyType;
        }

        public void setPolicyType(String policyType) {
            this.policyType = policyType;
        }

        public List<PolicyEntryInfo> getEntries() {
            return entries;
        }

        public void setEntries(List<PolicyEntryInfo> entries) {
            this.entries = entries;
        }
    }

    public static class PolicyEntryInfo {
        private String resource;

        private List<String> actions;

        private List<String> sourceIps;

        private String decision;

        public static PolicyEntryInfo of(String resource, List<String> actions, List<String> sourceIps,
            String decision) {
            PolicyEntryInfo policyEntryInfo = new PolicyEntryInfo();
            policyEntryInfo.setResource(resource);
            policyEntryInfo.setActions(actions);
            policyEntryInfo.setSourceIps(sourceIps);
            policyEntryInfo.setDecision(decision);
            return policyEntryInfo;
        }

        public String getResource() {
            return resource;
        }

        public void setResource(String resource) {
            this.resource = resource;
        }

        public List<String> getActions() {
            return actions;
        }

        public void setActions(List<String> actions) {
            this.actions = actions;
        }

        public List<String> getSourceIps() {
            return sourceIps;
        }

        public void setSourceIps(List<String> sourceIps) {
            this.sourceIps = sourceIps;
        }

        public String getDecision() {
            return decision;
        }

        public void setDecision(String decision) {
            this.decision = decision;
        }
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public List<PolicyInfo> getPolicies() {
        return policies;
    }

    public void setPolicies(List<PolicyInfo> policies) {
        this.policies = policies;
    }
}
