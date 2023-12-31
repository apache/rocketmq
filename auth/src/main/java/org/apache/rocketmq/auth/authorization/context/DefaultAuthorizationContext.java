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
package org.apache.rocketmq.auth.authorization.context;

import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.common.action.Action;

public class DefaultAuthorizationContext extends AuthorizationContext {

    private Subject subject;

    private Resource resource;

    private List<Action> actions;

    private String sourceIp;

    public static DefaultAuthorizationContext of(Subject subject, Resource resource, Action action, String sourceIp) {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(Collections.singletonList(action));
        context.setSourceIp(sourceIp);
        return context;
    }

    public static DefaultAuthorizationContext of(Subject subject, Resource resource, List<Action> actions,
        String sourceIp) {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(actions);
        context.setSourceIp(sourceIp);
        return context;
    }

    public String getSubjectKey() {
        return this.subject != null ? this.subject.toSubjectKey() : null;
    }

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }
}
