---
name: ISSUE_TEMPLATE
about: Describe this issue template's purpose here.

---

The issue tracker is used for bug reporting purposes **ONLY** whereas feature request needs to follow the [RIP process](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal). To avoid unnecessary duplication, please check whether there is a previous issue before filing a new one.

It is recommended to start a discussion thread in the [mailing lists](http://rocketmq.apache.org/about/contact/) in cases of discussing your deployment plan, API clarification, and other non-bug-reporting issues.
We welcome any friendly suggestions, bug fixes, collaboration, and other improvements.

Please ensure that your bug report is clear and self-contained. Otherwise, it would take additional rounds of communication, thus more time, to understand the problem itself.

Generally, fixing an issue goes through the following steps:
1. Understand the issue reported;
1. Reproduce the unexpected behavior locally;
1. Perform root cause analysis to identify the underlying problem;
1. Create test cases to cover the identified problem;
1. Work out a solution to rectify the behavior and make the newly created test cases pass;
1. Make a pull request and go through peer review;

As a result, it would be very helpful yet challenging if you could provide an isolated project reproducing your reported issue. Anyway, please ensure your issue report is informative enough for the community to pick up. At a minimum, include the following hints:

**BUG REPORT**

1. Please describe the issue you observed:

- What did you do (The steps to reproduce)?

- What is expected to see?

- What did you see instead?

2. Please tell us about your environment:

3. Other information (e.g. detailed explanation, logs, related issues, suggestions on how to fix, etc):

**FEATURE REQUEST**

1. Please describe the feature you are requesting.

2. Provide any additional detail on your proposed use case for this feature.

2. Indicate the importance of this issue to you (blocker, must-have, should-have, nice-to-have). Are you currently using any workarounds to address this issue?

4. If there are some sub-tasks involved, use -[] for each sub-task and create a corresponding issue to map to the sub-task:

- [sub-task1-issue-number](example_sub_issue1_link_here): sub-task1 description here, 
- [sub-task2-issue-number](example_sub_issue2_link_here): sub-task2 description here,
- ...
