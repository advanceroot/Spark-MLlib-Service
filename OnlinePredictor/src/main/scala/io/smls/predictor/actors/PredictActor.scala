package io.smls.predictor.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorIdentity, ActorInitializationException,
        ActorLogging, ActorRef, Identify, OneForOneStrategy, Props, ReceiveTimeout, Terminated}
import akka.routing.RoundRobinPool
import io.smls.predictor.message.{PredictRequest, RegisterRequest, RegisterResponse}
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.duration._

/**
  * Predict actor.
  *
  * States of this actor:
  * {{{
  *
  *           [Init]
  *             |
  *       >>> Req-Identify
  *             |
  *             v
  *          [Identify]
  *             |
  *       <<< Rsp-Identify
  *       >>> Req-Register
  *             |
  *             v
  *         [Register]
  *             |
  *             v
  *        <<< Rsp-Register
  *             |
  *             v
  *         [Active]
  *
  * }}}
  */
class PredictActor(val ssc:StreamingContext,
                    val appName:String,
                   val remoteActorPath:String,
                   val jarPath:String,
                   val modelClassName:String,
                   val modelPath:String) extends Actor with ActorLogging{
  var identifyRetry = 0
  val maxIdentifyRetry = 5

  var registerRetry = 0
  val maxRegisterRetry = 5

  val routerStrategy = OneForOneStrategy(){
    case initException:ActorInitializationException => {
      log.warning("Router Got ActorInitializationException, shutdown!!!");
      initException.printStackTrace()
      shutdown(s"Fail to create PredictWorker: ${initException}")
      Escalate
    }
    case _:Exception => {
      log.warning("Router Got Exception, Restart the Actor");
      Restart
    }
  }

  var router:ActorRef = null

  object IdentifyTimeout extends ReceiveTimeout
  object RegisterTimeout extends ReceiveTimeout

  val IdentifyId = "PredictActor"

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info("[PredictActor] preStart()")

    //go!
    sendIdentifyRequest()
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    log.info("[PredictActor] postStop()")
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.info("[PredictActor] preRestart")
  }

  @scala.throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info("[PredictActor] postRestart")
  }

  def sendIdentifyRequest():Unit = {
    context.actorSelection(remoteActorPath) ! Identify(IdentifyId)
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3.seconds, self, IdentifyTimeout)
  }

  def sendRegisterRequest(remoteActor: ActorRef):Unit = {
    remoteActor ! (new RegisterRequest(context.self.path.toString))
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3.seconds, self, RegisterTimeout)
  }

  override def receive: Receive = identify

  def identify():Actor.Receive = {
    case ActorIdentity(`IdentifyId`, Some(actor)) => {
      log.info(s"Identify success, start register (remote actor = ${actor})")
      //watch the querier
      context.watch(actor)
      sendRegisterRequest(actor) //start register
      context.become(register(actor))
    }

    case ActorIdentity(`IdentifyId`, None) => {
      shutdown(s"Identify failed, can not find the remote actor")
    }

    case IdentifyTimeout => {
      if (identifyRetry < maxIdentifyRetry){
        identifyRetry += 1
        log.warning(s"Identify timeout, retry...(${identifyRetry})")
        sendIdentifyRequest()
      } else {
        shutdown(s"Identify timeout, reach the max retry setting")
      }
    }

    case unknown => {
      log.warning(s"PredictActor[identify] rcv unknown msg ${unknown}")
    }
  }


  def register(remoteActor: ActorRef):Actor.Receive = {
    case regRsp:RegisterResponse => {
      if (regRsp.getStatus == 200) {
        log.info(s"Register success, become active...")
        router = context.actorOf(
          RoundRobinPool(nrOfInstances = 2, supervisorStrategy = routerStrategy).props(
            PredictWorker.props(ssc.sparkContext, jarPath, modelClassName, modelPath)),
          "PredictRouter")

        //For akka 2.2.5
        /*router = context.actorOf(
          PredictWorker.props(sc, jarPath, modelClassName, modelPath).withRouter(
            RoundRobinRouter(nrOfInstances = 10, supervisorStrategy = routerStrategy)
          ), "PredictRouter")*/

        context.become(active(remoteActor))
      } else {
        shutdown(s"Register failed (response code = ${regRsp.getStatus})")
      }

    }

    case RegisterTimeout => {
      if (registerRetry < maxRegisterRetry){
        registerRetry += 1
        log.warning(s"Register timeout, retry...(${registerRetry})")
        sendRegisterRequest(remoteActor)
      } else {
        shutdown(s"Register timeout, reach the max retry setting")
      }
    }

    case term:Terminated => {
      shutdown(s"PredictActor rcv remote actor terminated (${term.actor})")
    }

    case unknown => {
      log.warning(s"PredictActor[register] rcv unknown msg ${unknown}")
    }
  }

  def active(remoteActor: ActorRef):Actor.Receive = {
    case request: PredictRequest =>{
      log.info(s"->REQ(${request.getRequestId})")

      router.forward(request)
      log.info(s"forward completed")
    }

    case term:Terminated => {
      shutdown(s"PredictActor rcv remote actor terminated (${term.actor})")
    }

    case unknown => {
      log.warning(s"PredictActor[active] rcv unknown msg ${unknown}")
    }
  }

  def shutdown(msg:String): Unit ={
    log.error("Shutdown PredictActor >>> "+msg)
    context.stop(self)
    import scala.concurrent.ExecutionContext.Implicits.global
    context.system.shutdown()
    ssc.stop()
    log.warning("==========================================")
    log.warning("       EXIT the PredictServer!            ")
    log.warning("==========================================")
    java.lang.Runtime.getRuntime.exit(0)
  }
}

object PredictActor{
  def props(
           ssc:StreamingContext,
           appName:String,
            remoteActorPath:String,
            jarPath:String,
            modelClassName:String,
            modelPath:String):Props = Props(new PredictActor(ssc, appName, remoteActorPath, jarPath, modelClassName, modelPath))
}
