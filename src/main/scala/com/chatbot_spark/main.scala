package com.chatbot_spark

// spark
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SparkSession, Dataset}

// shell command
import sys.process._
import java.util.concurrent._

// akka specific
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
// akka http specific
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
// spray specific (JSON marshalling)
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
// cors
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

object main extends App {
  println("Spark-Consumer Server has started")
  val conf = new SparkConf().setMaster("local[*]").setAppName("spark-consumer")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
  import spark.implicits._
  sc.getConf.getAll.foreach(println)

  var time = 1
  val t = new java.util.Timer()
  val task = new java.util.TimerTask {
    def run() = {
      //val result = "mkdir /Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/".concat(time.toString()).!!
      val result = "aws s3 sync /home/ec2-user/chatbot/stream s3://data-engineering-fintech/chatbot/stream".!!
      println(result)
      println(time)
      time += 1
      if (time == 10) {
        t.cancel()
      }
    }
  }
  t.schedule(task, 1000L, 1000L)

  val streamDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ec2-15-164-95-216.ap-northeast-2.compute.amazonaws.com:9092").option("subscribe", "chatbot").load()
  val streamResult = streamDf.selectExpr("CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp as DATE)").as[(String, String, String)]
  streamResult.coalesce(1)
              .writeStream.outputMode("append")
              .format("json")
              .option("checkpointLocation","/home/ec2-user/chatbot/streaming/checkpointLocation")
              .option("path", "/home/ec2-user/chatbot/stream/json")
              .start()
              .awaitTermination()

//  val streamDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ec2-15-164-95-216.ap-northeast-2.compute.amazonaws.com:9092").option("subscribe", "chatbot").load()
//  val streamResult = streamDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
//  streamResult.coalesce(1)
//              .writeStream.outputMode("append")
//              .format("json")
//              .option("checkpointLocation","/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/streaming/checkpointLocation")
//              .option("path", "/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/chatbot/log/stream/json")
//              .start()
//              .awaitTermination()


//  streamResult.coalesce(1)
//              .writeStream
//              .outputMode("append")
//              .format("console.log")
//              .start()
//              .awaitTermination()

  val sourceRDD = sc.textFile("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/Files/words.txt")
  sourceRDD.take(1).foreach(println)

  final case class User(id: Long, name: String, email: String)

  implicit val actorSystem = ActorSystem(Behaviors.empty, "chatbotSpark")
  implicit val userMarshaller: spray.json.RootJsonFormat[User] = jsonFormat3(User.apply)

  val getUser = get {
    concat(
      path("hello") {
        complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "Hello world from scala akka http server!"))
      },
      path("user" / LongNumber) {
        userid => {
          println("get user by id : " + userid)
          userid match {
            case 1 => complete(User(userid, "Testuser", "test@test.com"  + sourceRDD.name))
            case _ => complete(StatusCodes.NotFound)
          }
        }
      },
      path("runSparkBatch") {
        val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "ec2-15-164-95-216.ap-northeast-2.compute.amazonaws.com:9092").option("subscribe", "chatbot").option("startingOffsets","earliest").load()
        df.dtypes.foreach(println)

        val selectedDf = df.selectExpr("CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(timestamp AS DATE)").as[(String, String, String)]
        // csv
        //result.write.partitionBy("timestamp").mode("overwrite").json("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/chatbot/batch/csv")
        selectedDf.write.partitionBy("timestamp").mode("overwrite").csv("/home/ec2-user/send/S3/chatbot/batch/csv")

        // json
        //result.write.partitionBy("timestamp").mode("overwrite").json("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/chatbot/batch/json")
        selectedDf.write.partitionBy("timestamp").mode("overwrite").json("/home/ec2-user/send/S3/chatbot/batch/json")

        val result:String = "aws s3 sync /home/ec2-user/send/S3/chatbot/batch s3://data-engineering-fintech/chatbot/batch".!!

        //val parquetResult = df.selectExpr("CAST(value AS STRING)", "CAST(topic AS STRING)", "timestamp").as[(String, String, String)]
        //parquetResult.coalesce(1).write.mode("overwrite").parquet("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/keypair/chatbot/log/parquet")

        complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "Spark Batch has been run successfully"))
      }
    )
  }

  val createUser = post {
    path("user") {
      entity(as[User]) {
        user => {
          println("save user")
          complete(User(user.id, "Testuser", "test@test.com"))
        }
      }
    }
  }

  val updateUser = put {
    path("user") {
      entity(as[User]) {
        user => {
          println("update user")
          complete(User(user.id, "Testuser", "test@test.com"))
        }
      }
    }
  }

  val deleteUser = delete {
    path("user" / LongNumber) {
      userid => {
        println(s"user ${userid}")
        complete(User(userid, "Testuser", "test@test.com"))
      }
    }
  }

  val route = cors() {
    concat(getUser, createUser, updateUser, deleteUser)
  }

  val bindFuture = Http().newServerAt("localhost", 8081).bind(route)
}