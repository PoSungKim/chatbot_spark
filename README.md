# chatbot_spark


### [Termianl]
```bash
sbt assembly

spark-submit \
  --class com.chatbot_spark.main \
  --master "local[*]" \
  --deploy-mode client \
  chatbot_spark-assembly-0.1.jar
```
