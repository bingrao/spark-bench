#!/bin/bash

# ############################################################### #
# PLEASE SET THE FOLLOWING VARIABLES TO REFLECT YOUR ENVIRONMENT  #
# ############################################################### #

# set this to the directory where Spark is installed in your environment, for example: /opt/spark-spark-2.1.0-bin-hadoop2.6
export SPARK_HOME="/home/bing/app/spark"

# set this to the master for your environment, such as local[2], yarn, 10.29.0.3, etc.
export SPARK_MASTER_HOST="spark://192.168.35.1:7077"
