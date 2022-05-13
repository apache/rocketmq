## Apache RocketMQ 
[![Build Status](https://travis-ci.org/apache/rocketmq.svg?branch=master)](https://travis-ci.org/apache/rocketmq) [![Coverage Status](https://coveralls.io/repos/github/apache/rocketmq/badge.svg?branch=master)](https://coveralls.io/github/apache/rocketmq?branch=master)
[![CodeCov](https://codecov.io/gh/apache/rocketmq/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://rocketmq.apache.org/dowloading/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq.svg)](http://isitmaintained.com/project/apache/rocketmq "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq.svg)](http://isitmaintained.com/project/apache/rocketmq "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)

**[Apache RocketMQ](https://rocketmq.apache.org) is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.**


It offers a variety of features:

* Messaging patterns including publish/subscribe, request/reply and streaming
* Financial grade transactional message
* Built-in fault tolerance and high availability configuration options base on [DLedger](https://github.com/openmessaging/openmessaging-storage-dledger)
* Built-in message tracing capability, also support opentracing
* Versatile big-data and streaming ecosystem integration
* Message retroactivity by time or offset
* Reliable FIFO and strict ordered messaging in the same queue
* Efficient pull and push consumption model
* Million-level message accumulation capacity in a single queue
* Multiple messaging protocols like gRPC, MQTT, JMS and OpenMessaging
* Flexible distributed scale-out deployment architecture
* Lightning-fast batch message exchange system
* Various message filter mechanics such as SQL and Tag
* Docker images for isolated testing and cloud isolated clusters
* Feature-rich administrative dashboard for configuration, metrics and monitoring
* Authentication and authorization
* Free open source connectors, for both sources and sinks
* Lightweight real-time computing
----------

## Apache RocketMQ Community
* [RocketMQ Streams](https://github.com/apache/rocketmq-streams): A lightweight stream computing engine based on RocketMQ.
* [RocketMQ Flink](https://github.com/apache/rocketmq-flink): The RocketMQ connector of Flink that supports source and sink connector in data stream and Table.
* [RocketMQ Client CPP](https://github.com/apache/rocketmq-client-cpp): RocketMQ CPP Client.
* [RocketMQ Client Go](https://github.com/apache/rocketmq-client-go): RocketMQ Go Client.
* [RocketMQ Client Python](https://github.com/apache/rocketmq-client-python)
* [RocketMQ Client Nodejs](https://github.com/apache/rocketmq-client-nodejs)
* [RocketMQ Spring](https://github.com/apache/rocketmq-spring)
* [RocketMQ Exporter](https://github.com/apache/rocketmq-exporter)
* [RocketMQ Operator](https://github.com/apache/rocketmq-operator): Provides a way to run an RocketMQ cluster on Kubernetes.
* [RocketMQ Docker](https://github.com/apache/rocketmq-docker): The Git repo of the Docker Image for Apache RocketMQ.
* [RocketMQ Dashboard](https://github.com/apache/rocketmq-dashboard): Operation and maintenance console of RocketMQ.
* [RocketMQ Connect](https://github.com/apache/rocketmq-connect): A tool for scalably and reliably streaming data between Apache RocketMQ and other systems.
* [RocketMQ MQTT](https://github.com/apache/rocketmq-mqtt): A new MQTT protocol architecture model, based on which RocketMQ can better support messages from terminals such as IoT devices and Mobile APP.
* [RocketMQ Incubating Community Projects](https://github.com/apache/rocketmq-externals): Apache rocketmq is incubating the ecological project warehouse, including Spark, ES, Beats and other connectors.
* [rocketmq-site](https://github.com/apache/rocketmq-site)

----------
## Learn it & Contact us
* Mailing Lists: <https://rocketmq.apache.org/about/contact/>
* Home: <https://rocketmq.apache.org>
* Docs: <https://rocketmq.apache.org/docs/quick-start/>
* Issues: <https://github.com/apache/rocketmq/issues>
* Rips: <https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal>
* Ask: <https://stackoverflow.com/questions/tagged/rocketmq>
* Slack: <https://rocketmq-invite-automation.herokuapp.com/>
 

----------



## Contributing
We always welcome new contributions, whether for trivial cleanups, [big new features](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal) or other material rewards, more details see [here](http://rocketmq.apache.org/docs/how-to-contribute/).
 
----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation


----------
## Export Control Notice
This distribution includes cryptographic software. The country in which you currently reside may have
restrictions on the import, possession, use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws, regulations and policies concerning
the import, possession, or use, and re-export of encryption software, to see if this is permitted. See
<http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this
software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software
using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache
Software Foundation distribution makes it eligible for export under the License Exception ENC Technology
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for
both object code and source code.

The following provides more details on the included cryptographic software:

This software uses Apache Commons Crypto (https://commons.apache.org/proper/commons-crypto/) to
support authentication, and encryption and decryption of data sent across the network between
services.
