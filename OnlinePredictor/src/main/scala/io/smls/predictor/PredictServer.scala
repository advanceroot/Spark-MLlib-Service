package io.smls.predictor

import java.lang.Boolean
import java.util

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.smls.predictor.actors.{PredictActor, QueryActor}
import io.smls.predictor.message.PredictRequest
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._

/**
  * Runnable application of prediction. This application should be run in Spark environment.
  *
  * When the application start, open a connection to remote Akka address and register. When
  * predict request arrived, a work actor load model and use the model do predict and send
  * result back.
  *
  * This application take arguments as below:
  * {{{
  * --------------------------spark configuration------------------------------
  * [mandatory] -appName:String                  name of the application
  *
  * --------------------------runtime configuration----------------------------
  * [mandatory] -algoPluginPath:String           path of the plugin jar
  * [mandatory] -modelClass:String               class name of model
  * [mandatory] -modelPath:String                path of trained model
  *
  * --------------------------akka configuration-------------------------------
  * [mandatory] -remoteActorPath:String          AKKA address registered to
  * [mandatory] -threadParallel:Int              thread used = threadParallel*(number of cores)
  *
  * }}}
  *
  */

object PredictServer {
  def main(args: Array[String]): Unit = {
    val LOCAL_TEST_MODE = false //local test flag
    if (LOCAL_TEST_MODE){
      startQuerier("eth0")
      return
    }

    //parse arguments, all the arguments must be set and no NULL value
    def nextOption(map : Map[String, _], list: List[String]) : Map[String, _] = {
      list match {
        case Nil => map
        case "-appName" :: value :: tail =>
          nextOption(map ++ Map("appName" -> value), tail)
        case "-remoteActorPath" :: value :: tail =>
          nextOption(map ++ Map("remoteActorPath" -> value), tail)
        case "-algoPluginPath" :: value :: tail =>
          nextOption(map ++ Map("algoPluginPath" -> value), tail)
        case "-modelClass" :: value :: tail =>
          nextOption(map ++ Map("modelClass" -> value), tail)
        case "-modelPath" :: value :: tail =>
          nextOption(map ++ Map("modelPath" -> value), tail)
        /*case "-netInterface" :: value :: tail =>
          nextOption(map ++ Map("netInterface" -> value), tail)*/
        case "-threadParallel" :: value :: tail =>
          nextOption(map ++ Map("threadParallel" -> value), tail)
        case option :: tail => throw new IllegalArgumentException("Illegal option: " + option)
      }
    }

    val argMap = nextOption(Map(), args.toList)

    val appName = argMap("appName").toString
    val remoteActorPath = argMap("remoteActorPath").toString
    val jarPath = argMap("algoPluginPath").toString
    val modelClass = argMap("modelClass").toString
    val modelPath = argMap("modelPath").toString
    val netInterface = "eth0"/*argMap("netInterface").toString*/
    val threadParallel = Integer.valueOf(argMap("threadParallel").toString)


    //start predict server
    startPredictServer(remoteActorPath, netInterface, threadParallel, appName, jarPath, modelClass, modelPath)
  }

  def startPredictServer(remoteActorPath:String,
                         localInterface:String,
                         parallel:Int,
                         appName:String,
                         jarPath:String,
                         modelClass:String,
                         modelPath:String):Unit = {
    val localIP = Utils.queryLocalIPAddress(localInterface)
    if (localIP == null){
      throw new IllegalStateException("PredictServer can not find any usable IP, quit!")
    }


    val conf = new SparkConf()
    conf.setAppName("QueryClusterApp-code")


    val ssc = new StreamingContext(conf, Minutes(10))
    val sc = ssc.sparkContext

    val configMap = new util.HashMap[String, Object]()
    configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider") //AKKA: 2.3.16
    configMap.put("akka.remote.netty.tcp.hostname", localIP)
    configMap.put("akka.remote.netty.tcp.port", Integer.valueOf(0)) //port 0: let system choose automatically
    configMap.put("akka.actor.default-dispatcher.executor", "thread-pool-executor")
    configMap.put("akka.actor.default-dispatcher.thread-pool-executor.fixed-pool-size",
      Integer.valueOf(Runtime.getRuntime.availableProcessors * parallel))

    configMap.put("akka.actor.warn-about-java-serializer-usage", new Boolean(false))
    configMap.put("akka.remote.watch-failure-detector.heartbeat-interval", "10 s")
    configMap.put("akka.remote.watch-failure-detector.expected-response-after", "10 s")
    val system = ActorSystem("PredictSystem", ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load()))

    println(s"PredictSystem -----------> ${remoteActorPath}")
    val predictActor = system.actorOf(PredictActor.props(ssc, appName, remoteActorPath, jarPath, modelClass, modelPath), "predictor")
    println(s"PredictSystem(${predictActor}) is started - waiting for incoming messages...")

    ssc.awaitTermination()
  }



  //local test
  def startQuerier(localInterface:String): Unit = {
    val localIP = Utils.queryLocalIPAddress(localInterface)
    if (localIP == null){
      throw new IllegalStateException("PredictServer can not find any usable IP, quit!")
    }

    val configMap = new util.HashMap[String, Object]()
    configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider") //AKKA: 2.3.16
    configMap.put("akka.remote.netty.tcp.hostname", localIP)
    configMap.put("akka.remote.netty.tcp.port",Integer.valueOf(2560))
    configMap.put("akka.actor.warn-about-java-serializer-usage", new Boolean(false))

    val system = ActorSystem("QuerySystem", ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load()))
    val actor = system.actorOf(Props(classOf[QueryActor]), "querier")

    println(s"QuerySystem is started at:${actor} ")

    //Run waiting
    val conf = new SparkConf()
    conf.setAppName("QueryClusterApp-code")
    val ssc = new StreamingContext(conf, Minutes(10))

    import system.dispatcher
    var jobId = 0
    system.scheduler.schedule(1.second, 3000.millisecond) {
      jobId += 1
      println(s"Ticking....${jobId}")
      if (jobId > 30){
        ssc.stop(true)
        throw new IllegalStateException("Normal Quit!!!!")
      } else {
        actor ! (new PredictRequest(jobId, "{\"data\":["+jobId+"]}"))
      }

    }

    ssc.awaitTermination()
  }

}
