### Operating system tuning
Before the broker deployment, you must run **os.sh**, which is optimized for your operating system.

## Notice
### os.sh should be executed only once with the root permission.
### os.sh parameter settings is just for reference. You can tune it in your target host systems.


### Start broker
* Unix platform

  `nohup sh mqbroker &`

* Windows platform（Only support 64 bit）

  `mqbroker.exe`

### Shutodwn broker
  sh mqshutdown broker

### Start Nameserver
* Unix platform

  `nohup sh mqnamesrv &`

* Windows platform（Only support 64 bit）

  `mqnamesrv.exe`

### Shutdown Nameserver
    sh mqshutdown namesrv

### Update or create Topic
    sh mqadmin updateTopic -b 127.0.0.1:10911 -t TopicA

### Update or create subscription group
    sh mqadmin updateSubGroup -b 127.0.0.1:10911 -g SubGroupA