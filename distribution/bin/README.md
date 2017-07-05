### Operating system tuning
Before deploying broker servers, it's highly recommended to run **os.sh**, which is to optimize your operating system for better performance.

## Notice
### os.sh should be executed only once with root permission.
### os.sh parameter settings are for reference purpose only. You can tune them according to your target host configurations.


### Start broker
* Unix platform

  `nohup sh mqbroker &`

### Shutdown broker
  sh mqshutdown broker

### Start Nameserver
* Unix platform

  `nohup sh mqnamesrv &`

### Shutdown Nameserver
    sh mqshutdown namesrv

### Update or create Topic
    sh mqadmin updateTopic -b 127.0.0.1:10911 -t TopicA

### Update or create subscription group
    sh mqadmin updateSubGroup -b 127.0.0.1:10911 -g SubGroupA