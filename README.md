
<div align="right">
  <details>
    <summary >🌐 Language</summary>
    <div>
      <div align="center">
        <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=en">English</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=zh-CN">简体中文</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=zh-TW">繁體中文</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=ja">日本語</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=ko">한국어</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=hi">हिन्दी</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=th">ไทย</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=fr">Français</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=de">Deutsch</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=es">Español</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=it">Italiano</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=ru">Русский</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=pt">Português</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=nl">Nederlands</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=pl">Polski</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=ar">العربية</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=fa">فارسی</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=tr">Türkçe</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=vi">Tiếng Việt</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=id">Bahasa Indonesia</a>
        | <a href="https://openaitx.github.io/view.html?user=apache&project=rocketmq&lang=as">অসমীয়া</
      </div>
    </div>
  </details>
</div>

## Apache RocketMQ

[![Build Status][maven-build-image]][maven-build-url]
[![CodeCov][codecov-image]][codecov-url]
[![Maven Central][maven-central-image]][maven-central-url]
[![Release][release-image]][release-url]
[![License][license-image]][license-url]
[![Average Time to Resolve An Issue][percentage-of-issues-still-open-image]][percentage-of-issues-still-open-url]
[![Percentage of Issues Still Open][average-time-to-resolve-an-issue-image]][average-time-to-resolve-an-issue-url]
[![Twitter Follow][twitter-follow-image]][twitter-follow-url]

**[Apache RocketMQ](https://rocketmq.apache.org) is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.**


It offers a variety of features:

* Messaging patterns including publish/subscribe, request/reply and streaming
* Financial grade transactional message
* Built-in fault tolerance and high availability configuration options base on [DLedger Controller](docs/en/controller/quick_start.md)
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


## Quick Start

This paragraph guides you through steps of installing RocketMQ in different ways.
For local development and testing, only one instance will be created for each component.

### Run RocketMQ locally

RocketMQ runs on all major operating systems and requires only a Java JDK version 8 or higher to be installed.
To check, run `java -version`:
```shell
$ java -version
java version "1.8.0_121"
```

For Windows users, click [here](https://dist.apache.org/repos/dist/release/rocketmq/5.3.3/rocketmq-all-5.3.3-bin-release.zip) to download the 5.3.3 RocketMQ binary release,
unpack it to your local disk, such as `D:\rocketmq`.
For macOS and Linux users, execute following commands:

```shell
# Download release from the Apache mirror
$ wget https://dist.apache.org/repos/dist/release/rocketmq/5.3.3/rocketmq-all-5.3.3-bin-release.zip

# Unpack the release
$ unzip rocketmq-all-5.3.3-bin-release.zip
```

Prepare a terminal and change to the extracted `bin` directory:
```shell
$ cd rocketmq-all-5.3.3-bin-release/bin
```

**1) Start NameServer**

NameServer will be listening at `0.0.0.0:9876`, make sure that the port is not used by others on the local machine, and then do as follows.

For macOS and Linux users:
```shell
### start Name Server
$ nohup sh mqnamesrv &

### check whether Name Server is successfully started
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

For Windows users, you need set environment variables first:
- From the desktop, right click the Computer icon.
- Choose Properties from the context menu.
- Click the Advanced system settings link.
- Click Environment Variables.
- Add Environment `ROCKETMQ_HOME="D:\rocketmq"`. 

Then change directory to rocketmq, type and run:
```shell
$ mqnamesrv.cmd
The Name Server boot success...
```

**2) Start Broker**

For macOS and Linux users:
```shell
### start Broker
$ nohup sh bin/mqbroker -n localhost:9876 &

### check whether Broker is successfully started, eg: Broker's IP is 192.168.1.2, Broker's name is broker-a
$ tail -f ~/logs/rocketmqlogs/broker.log
The broker[broker-a, 192.169.1.2:10911] boot success...
```

For Windows users:
```shell
$ mqbroker.cmd -n localhost:9876
The broker[broker-a, 192.169.1.2:10911] boot success...
```

### Run RocketMQ in Docker

You can run RocketMQ on your own machine within Docker containers,
`host` network will be used to expose listening port in the container.

**1) Start NameServer**

```shell
$ docker run -it --net=host apache/rocketmq ./mqnamesrv
```

**2) Start Broker**

```shell
$ docker run -it --net=host --mount type=bind,source=/tmp/store,target=/home/rocketmq/store apache/rocketmq ./mqbroker -n localhost:9876
```

### Run RocketMQ in Kubernetes

You can also run a RocketMQ cluster within a Kubernetes cluster using [RocketMQ Operator](https://github.com/apache/rocketmq-operator).
Before your operations, make sure that `kubectl` and related kubeconfig file installed on your machine.

**1) Install CRDs**
```shell
### install CRDs
$ git clone https://github.com/apache/rocketmq-operator
$ cd rocketmq-operator && make deploy

### check whether CRDs is successfully installed
$ kubectl get crd | grep rocketmq.apache.org
brokers.rocketmq.apache.org                 2022-05-12T09:23:18Z
consoles.rocketmq.apache.org                2022-05-12T09:23:19Z
nameservices.rocketmq.apache.org            2022-05-12T09:23:18Z
topictransfers.rocketmq.apache.org          2022-05-12T09:23:19Z

### check whether operator is running
$ kubectl get pods | grep rocketmq-operator
rocketmq-operator-6f65c77c49-8hwmj   1/1     Running   0          93s
```

**2) Create Cluster Instance**
```shell
### create RocketMQ cluster resource
$ cd example && kubectl create -f rocketmq_v1alpha1_rocketmq_cluster.yaml

### check whether cluster resources is running
$ kubectl get sts
NAME                 READY   AGE
broker-0-master      1/1     107m
broker-0-replica-1   1/1     107m
name-service         1/1     107m
```

---
## Apache RocketMQ Community
* [RocketMQ Streams](https://github.com/apache/rocketmq-streams): A lightweight stream computing engine based on Apache RocketMQ.
* [RocketMQ Flink](https://github.com/apache/rocketmq-flink): The Apache RocketMQ connector of Apache Flink that supports source and sink connector in data stream and Table.
* [RocketMQ APIs](https://github.com/apache/rocketmq-apis): RocketMQ protobuf protocol.
* [RocketMQ Clients](https://github.com/apache/rocketmq-clients): gRPC/protobuf-based RocketMQ clients.
* RocketMQ Remoting-based Clients
	 - [RocketMQ Client CPP](https://github.com/apache/rocketmq-client-cpp)
	 - [RocketMQ Client Go](https://github.com/apache/rocketmq-client-go)
	 - [RocketMQ Client Python](https://github.com/apache/rocketmq-client-python)
	 - [RocketMQ Client Nodejs](https://github.com/apache/rocketmq-client-nodejs)
* [RocketMQ Spring](https://github.com/apache/rocketmq-spring): A project which helps developers quickly integrate Apache RocketMQ with Spring Boot.
* [RocketMQ Exporter](https://github.com/apache/rocketmq-exporter): An Apache RocketMQ exporter for Prometheus.
* [RocketMQ Operator](https://github.com/apache/rocketmq-operator): Providing a way to run an Apache RocketMQ cluster on Kubernetes.
* [RocketMQ Docker](https://github.com/apache/rocketmq-docker): The Git repo of the Docker Image for Apache RocketMQ.
* [RocketMQ Dashboard](https://github.com/apache/rocketmq-dashboard): Operation and maintenance console of Apache RocketMQ.
* [RocketMQ Connect](https://github.com/apache/rocketmq-connect): A tool for scalably and reliably streaming data between Apache RocketMQ and other systems.
* [RocketMQ MQTT](https://github.com/apache/rocketmq-mqtt): A new MQTT protocol architecture model, based on which Apache RocketMQ can better support messages from terminals such as IoT devices and Mobile APP.
* [RocketMQ EventBridge](https://github.com/apache/rocketmq-eventbridge): EventBridge make it easier to build a event-driven application.
* [RocketMQ Incubating Community Projects](https://github.com/apache/rocketmq-externals): Incubator community projects of Apache RocketMQ, including [logappender](https://github.com/apache/rocketmq-externals/tree/master/logappender), [rocketmq-ansible](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-ansible), [rocketmq-beats-integration](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-beats-integration), [rocketmq-cloudevents-binding](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-cloudevents-binding), etc.
* [RocketMQ Site](https://github.com/apache/rocketmq-site): The repository for Apache RocketMQ website.
* [RocketMQ E2E](https://github.com/apache/rocketmq-e2e): A project for testing Apache RocketMQ, including end-to-end, performance, compatibility tests.


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

[maven-build-image]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml/badge.svg
[maven-build-url]: https://github.com/apache/rocketmq/actions/workflows/maven.yaml
[codecov-image]: https://codecov.io/gh/apache/rocketmq/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/apache/rocketmq
[maven-central-image]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg
[maven-central-url]: http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq
[release-image]: https://img.shields.io/badge/release-download-orange.svg
[release-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[average-time-to-resolve-an-issue-image]: http://isitmaintained.com/badge/resolution/apache/rocketmq.svg
[average-time-to-resolve-an-issue-url]: http://isitmaintained.com/project/apache/rocketmq
[percentage-of-issues-still-open-image]: http://isitmaintained.com/badge/open/apache/rocketmq.svg
[percentage-of-issues-still-open-url]: http://isitmaintained.com/project/apache/rocketmq
[twitter-follow-image]: https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social
[twitter-follow-url]: https://twitter.com/intent/follow?screen_name=ApacheRocketMQ
