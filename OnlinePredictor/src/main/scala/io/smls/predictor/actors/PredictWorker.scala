package io.smls.predictor.actors

import java.io.File
import java.net.{URL, URLClassLoader}

import akka.actor.{Actor, ActorLogging, Props}
import io.smls.base.AbstractModel
import io.smls.predictor.message.{PredictRequest, PredictResponse}
import org.apache.spark.SparkContext

/**
  * Worker actor.
  *
  * Do the work of prediction.
  */
class PredictWorker(val sparkContext:SparkContext,
                    val jarPath:String,
                    val modelClassName:String,
                    val modelPath:String) extends Actor with ActorLogging{

  var model:AbstractModel[_,_] = null

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    println("[PredictWorker] preStart()")
    val classLoader = new URLClassLoader(Array[URL](new File(jarPath).toURI.toURL), this.getClass.getClassLoader)
    model = classLoader.loadClass(modelClassName).newInstance().asInstanceOf[AbstractModel[_,_]]
    model.onLoadModel(sparkContext, modelPath)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    println("[PredictWorker] postStop()")
    model.onUnload(sparkContext)
  }

  override def receive: Receive = {
    case request: PredictRequest =>{
      log.info(s"[Worker] -> request(${request.getRequestId})")
      try {
        val result = model.onPredict(sparkContext, request.getData)
        sender ! (new PredictResponse(request.getRequestId, result))
        log.info(s"Result =${result}")
      } catch {
        case ex:Exception => {
          sender ! (new PredictResponse(request.getRequestId, "Exception:" + ex.getMessage()))
        }
      }
    }
  }
}

object PredictWorker{
  def props(sparkContext:SparkContext,
            jarPath:String,
            modelClassName:String,
            modelPath:String):Props = Props(new PredictWorker(sparkContext, jarPath, modelClassName, modelPath))
}
