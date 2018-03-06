package io.smls.predictor.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import io.smls.predictor.message.{PredictRequest, PredictResponse, RegisterRequest, RegisterResponse}

/**
  * Query actor.
  *
  * Use as a query test case only.
  */
class QueryActor extends Actor with ActorLogging{
  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info("[QueryActor] preStart()")
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.info("[QueryActor] postStop()")
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.info("[QueryActor] preRestart")
  }

  @scala.throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info("[QueryActor] postRestart")
  }

  override def receive: Receive = register

  def register():Actor.Receive = {
    case regRequest:RegisterRequest => {
      log.info(s"QueryActor recv Register request, path = ${regRequest.getPath}, sender = ${sender}")
      sender ! (new RegisterResponse(200, "Reg Ok"))

      context.watch(sender)
      context.become(active(sender))
    }

    case term:Terminated => {
      log.info(s"QueryActor rcv remote actor terminated (${term.actor})")
    }

    case x => {
      log.warning("QueryActor rcv unknown msg>>> " + x)
    }
  }

  def active(predictActor: ActorRef):Actor.Receive = {
    case predictRequest:PredictRequest => {
      log.info(s"QueryActor recv Predict request")
      predictActor ! predictRequest
    }

    case predictResponse:PredictResponse => {
      log.info(s"QueryActor recv Predict response(${predictResponse.getRequestId}, ${predictResponse.getResult})")
    }

    case term:Terminated => {
      log.info(s"QueryActor rcv remote actor terminated (${term.actor})")
      shutdown("Predict actor is terminated!!!!")
    }

    case _ => {
      log.warning("QueryActor rcv unknown msg")
    }
  }

  def shutdown(msg:String): Unit ={
    log.error("Shutdown QueryActor >>> "+msg)
    context.stop(self)
    context.system.shutdown()
  }
}
