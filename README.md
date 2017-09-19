## Angelia
Angelia provides a single API for most network related service that uses pluggable transports and codecs. The Angelia API provides the ability for making synchronous, asynchronous, oneway remote calls, push and pull callbacks. The intention is to allow for the use of different transports to fit different needs, yet still maintain the same API for making the remote invocations and only requiring configuration changes, not code changes.

Angelia is a standalone project, separate from the Alibaba RocketMQ and Jukola project, but will be the framework used for many of the projects and components when making remote calls. Angelia is included in the recent releases of the Alibaba Jukola and can be run as a service within the container as well.

### Features
The features available with Angelia are:

#### 1. Pluggable transports – can use different protocol transports the same remoting API.
Provided transports:

##### **MVP, a custom-build Minimum Viable Protocol**
##### **HTTP2**
     
#### 2. Pluggable codecs – can use different codecs to convert the invocation payloads into desired data format for wire transfer.

#### 3. Pluggable serialization - can use different serialization implementations for data streams.

Provided serialization implementations:

##### **MessagePack**
##### **Kryo**
##### **Fastjson**

#### 4. Data Compression - can use compression codec for compresssion of large payloads.

All the features within Angelia were created with ease of use and extensibility in mind. If you have a suggestion for a new feature or an improvement to a current feature, please let me know.








## Documentation
### [Quick Start]()
### [User Guide]()
### [Developer Guide]()
### [Release Note]()



----------

## Contributing
We are always very happy to have contributions, whether for trivial cleanups,big new features or other material rewards. more details see [here](CONTRIBUTING.md) 
 
----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) 2010-2016 Alibaba Group Holding Limited