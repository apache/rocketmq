## How To Contribute

We are always very happy to have contributions, whether for trivial cleanups or big new features.
We want to have high quality, well documented codes for each programming language, as well as the surrounding [ecosystem](https://github.com/apache/rocketmq-externals) of integration tools that people use with RocketMQ.

Nor is code the only way to contribute to the project. We strongly value documentation, integration with other projects, and gladly accept improvements for these aspects.

Recommend reading:
 * [Contributors Tech Guide](http://www.apache.org/dev/contributors)
 * [Get involved!](http://www.apache.org/foundation/getinvolved.html)

## Contributing code

To submit a change for inclusion, please do the following:

#### If the change is non-trivial please include some unit tests that cover the new functionality.
#### If you are introducing a completely new feature or API it is a good idea to start a [RIP](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal) and get consensus on the basic design first.
#### It is our job to follow up on patches in a timely fashion. Nag us if we aren't doing our job (sometimes we drop things).

### Squash commits

If you have a pull request on GitHub, and updated more than once, it's better to squash all commits.

1. Identify how many commits you made since you began: ``git log``.
2. Squash these commits by N: ``git rebase -i HEAD~N`` .
3. Leave "pick" tag in the first line.
4. Change all other commits from "pick" to "fixup".
5. Then do "force push" to overwrite remote history: ``git push -u origin ROCKETMQ-9999 --force``
6. All your changes are now in a single commit, that would be much better for review.

More details of squash can be found at [stackoverflow](https://stackoverflow.com/questions/5189560/squash-my-last-x-commits-together-using-git).

## Becoming a Committer

We are always interested in adding new contributors. What we look for are series of contributions, good taste and ongoing interest in the project. If you are interested in becoming a committer, please let one of the existing committers know and they can help you walk through the process.

Nowadays, we have several important contribution points:
#### Wiki & JavaDoc
#### RocketMQ SDK(C++\.Net\Php\Python\Go\Node.js)
#### RocketMQ Connectors

##### Prerequisites
If you want to contribute to the above listed points, you must abide by the following prerequisites:

###### Readability - API must have Javadoc, and some very important methods must also have Javadoc
###### Testability - Above 80% unit test coverage for the main process
###### Maintainability - Comply with our [checkstyle spec](style/rmq_checkstyle.xml), and at least a 3-month update frequency
###### Deployability - We encourage you to deploy into [maven repository](http://search.maven.org/)
