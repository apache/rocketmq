/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;


/**
 * 长轮询请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class ManyPullRequest {
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<PullRequest>();


    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }


    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }


    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }
}
