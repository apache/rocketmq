#!/bin/sh

#
# $Id: producer.sh 1831 2013-05-16 01:39:51Z shijia.wxr $
#
sh ./runclass.sh -Dcom.alibaba.rocketmq.client.sendSmartMsg=true com.alibaba.rocketmq.example.benchmark.Producer $@
