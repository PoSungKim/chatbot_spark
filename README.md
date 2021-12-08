# chatbot_spark


### [Termianl]
```bash
sbt assembly

spark-submit \
  --class com.chatbot_spark.main \
  --master "local[*]" \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
  chatbot_spark-assembly-0.1.jar
```
