#!/bin/bash
export HADOOP_CLASSPATH=/usr/lib/hbase/*:/usr/lib/hbase/lib/*
while true; do
	hadoop jar mapReduceSensor_v5.2.2.jar Design.Batch.BatchLayerDriver /usr/cloudera/mockData.txt
done