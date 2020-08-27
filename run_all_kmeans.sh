#!/bin/sh
PWD=`pwd`
BIN=${PWD}/bin
EXAMPLE=${PWD}/examples
bench_name="kmeans"
for nums in 10
do
  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-all-${nums}.conf
  sleep 10s
  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-soda-${nums}.conf
  sleep 10s
  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-none-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-all-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-soda-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-none-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-all-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-soda-${nums}.conf
#  sleep 10s
#  ${BIN}/spark-bench.sh ${EXAMPLE}/${bench_name}/${bench_name}-run-none-${nums}.conf
#  sleep 10s
done