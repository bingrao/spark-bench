#!/bin/sh
LogPath=$1
Slaves=$(cat "${SPARK_HOME}"/conf/slaves | grep "^c[0-9]")
SinkDir="/scratch/data_bing/spark/csvsink/"
cluster_user="bing"
applications=$(ls "${SinkDir}" | grep  "app-" | cut -d "." -f1 | sort | uniq)


getlogs(){
  application_ID=$1
  #Retrival yarn log
  LOGS_DIR="${LogPath}/${application_ID}"
  mkdir -p ${LOGS_DIR}
  #yarn logs -applicationId ${application_ID} >>${LOGS_DIR}/${application_ID}_yarn.logs

  #Retrival Sink Data
  mkdir -p ${LOGS_DIR}/master
  cp ${SinkDir}/${application_ID}* ${LOGS_DIR}/master/
  rm -fr ${SinkDir}/${application_ID}*
  sudo drop-caches

  for slave in ${Slaves}
  do
     tmp_Dir="${LOGS_DIR}/${slave}"
     mkdir -p ${tmp_Dir}
     scp ${cluster_user}@$slave:${SinkDir}/${application_ID}* ${tmp_Dir}/
     ssh ${cluster_user}@$slave rm -fr ${SinkDir}/${application_ID}*
  done

  #Copy history log from HDFS
  #hdfs dfs -copyToLocal /logs/spark-events/${application_ID}* ${LOGS_DIR}/${application_ID}_history_event.logs
}


for app in ${applications}
do
  getlogs $app
done

for slave in ${Slaves}
do
  ssh ${cluster_user}@$slave sudo drop-caches
done