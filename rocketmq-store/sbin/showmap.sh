#!/bin/sh
ps ax | grep -i 'com.taobao.metaq' |grep java | grep -v grep | awk '{print $1}' | xargs pmap |grep metastore