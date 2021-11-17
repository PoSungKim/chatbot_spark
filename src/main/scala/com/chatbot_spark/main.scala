package com.chatbot_spark

import org.apache.spark.SparkContext

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
  println("Hello World");

  val sparkContext = new SparkContext("local", "chatbot_spark");
  val sourceRDD = sparkContext.textFile("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/Files/words.txt")
  sourceRDD.take(1).foreach(println)

  final case class User(id: Long, name: String, email: String)

  implicit val actorSystem = ActorSystem(Behaviors.empty, "akka-http")
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
            case 1 => complete(User(userid, "Testuser", "test@test.com"))
            case _ => complete(StatusCodes.NotFound)
          }
        }
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

  val bindFuture = Http().newServerAt("localhost", 8080).bind(route)
}