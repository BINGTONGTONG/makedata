SPARK_HOME=$SPARK_BENCHMARK_HOME/tools/spark-2.3.0-bin-hadoop2.7/
$SPARK_HOME/bin/spark-submit --executor-cores 4  --num-executors 256  --conf spark.yarn.jars=hdfs:///spark_benchmark/dependency/spark230_jars/*.jar    --driver-memory 8g   --executor-memory 8g  \
     --master yarn --deploy-mode cluster \
     --conf spark.hadoop.yarn.timeline-service.enabled=false \
     --conf spark.rpc.message.maxSize=1024 \
     --conf spark.yarn.maxAppAttempts=1 \
     --conf spark.yarn.driver.memoryOverhead=48g \
     --conf spark.yarn.executor.memoryOverhead=4g \
     gen.py
