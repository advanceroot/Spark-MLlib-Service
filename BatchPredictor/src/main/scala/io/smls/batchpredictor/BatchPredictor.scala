package io.smls.batchpredictor

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}

import io.smls.base._
import io.smls.base.util.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Runnable application of batch prediction. This application should be run in Spark environment.
  * The completion of this app should be detected by Spark job tracker.
  *
  * This application take arguments as below:
  * {{{
  * --------------------------spark configuration------------------------------
  * [mandatory] -appName:String                  name of the application
  *
  * --------------------------plugin configuration----------------------------
  * [mandatory] -algoPluginPath:String           path of the plugin jar
  * [mandatory] -modelClass:String               class name of model
  * [mandatory] -dataParameterClass:String       class name of data parameter
  *
  * --------------------------data configuration----------------------------
  * [mandatory] -modelPath:String                path of trained model
  * [mandatory] -sourceParameter:String          source parameter
  * [mandatory] -dataParameter:String            data parameter
  *
  * --------------------------saving configuration----------------------------
  * [mandatory] -resultSavePath:String           saving path of result
  *
  * }}}
  *
  */
object BatchPredictor {
  private[this] def parseArgument(args:Array[String]):Map[String, _] = {
    def nextOption(map : Map[String, _], list: List[String]) : Map[String, _] = {
      list match {
        case Nil => map
        case "-appName" :: value :: tail =>
          nextOption(map ++ Map("appName" -> value), tail)
        case "-algoPluginPath" :: value :: tail =>
          nextOption(map ++ Map("algoPluginPath" -> value), tail)
        case "-modelClass" :: value :: tail =>
          nextOption(map ++ Map("modelClass" -> value), tail)
        case "-dataParameterClass" :: value :: tail =>
          nextOption(map ++ Map("dataParameterClass" -> value), tail)
        case "-modelPath" :: value :: tail =>
          nextOption(map ++ Map("modelPath" -> value), tail)
        case "-sourceParameter" :: value :: tail =>
          nextOption(map ++ Map("sourceParameter" -> value), tail)
        case "-dataParameter" :: value :: tail =>
          nextOption(map ++ Map("dataParameter" -> value), tail)
        case "-resultSavePath" :: value :: tail =>
          nextOption(map ++ Map("resultSavePath" -> value), tail)
        case option :: tail => throw new IllegalArgumentException("Illegal option: " + option)
      }
    }

    nextOption(Map(), args.toList)
  }

  def main(args: Array[String]): Unit = {
    //parse arguments
    val argMap = parseArgument(args)
    val TAG = argMap("appName").toString
    Log.d(TAG, s"Arguments: ${argMap}")

    val appName = argMap("appName").toString
    val jarPath = argMap("algoPluginPath").toString
    val modelClass = argMap("modelClass").toString
    val modelPath = argMap("modelPath").toString
    val dataParameterClass = argMap("dataParameterClass").toString
    val sourceParameterStr = argMap("sourceParameter").toString
    val dataParameterStr = argMap("dataParameter").toString
    val resultSavePath = argMap("resultSavePath").toString

    //spark configuration
    val conf = new SparkConf()
    conf/*.setMaster(masterAddress)*/.setAppName(appName)
    val sc = new SparkContext(conf)
    Log.d(TAG, s"BatchPredictor start to run application ${appName} (app id = ${sc.applicationId})")

    //load class from jar
    val classLoader = new URLClassLoader(Array[URL](new File(jarPath).toURI.toURL), this.getClass.getClassLoader)
    val dataParameter = classLoader.loadClass(dataParameterClass).newInstance().asInstanceOf[BatchPredictDataParameter]
    val model = classLoader.loadClass(modelClass).newInstance()/*.asInstanceOf[AbstractModel[_,_]]*/

    //---------------------------------------------------------------------------------------------------//
    //-------------------------------------------Unsafe Code---------------------------------------------//
    //---------------------------------------------------------------------------------------------------//
    //load model
    /**
      * public abstract void onLoadModel(
      * org.apache.spark.SparkContext,
      * java.lang.String);
      */
    val onLoadModelMethod = model.getClass.getDeclaredMethod("onLoadModel", sc.getClass, classOf[String])
    Log.d(TAG, s"onLoadModel() = ${onLoadModelMethod}")
    onLoadModelMethod.invoke(model, sc, modelPath)
    Log.d(TAG, s"Load model complete")

    //load raw rdd
    /** public org.apache.spark.rdd.RDD<?> onLoadBatchPredictData(
      *   org.apache.spark.SparkContext,
      *   io.smls.base.SourceParameter);*/
    val onLoadBatchPredictDataMethod = model.getClass.getDeclaredMethod("onLoadBatchPredictData",
      sc.getClass,
      classOf[SourceParameter])
    Log.d(TAG, s"onLoadBatchPredictData() = ${onLoadBatchPredictDataMethod}")

    val sourceMap = JSON.parseFull(sourceParameterStr).get.asInstanceOf[Map[String, _]]
    val typeArg = sourceMap("type").toString.toUpperCase
    if(!SourceType.values.map(_.toString).contains(typeArg)){
      throw new IllegalArgumentException("Unsupported source type:" + typeArg)
    }
    val sourceType = SourceType.withName(typeArg)
    val rawRdd = onLoadBatchPredictDataMethod.invoke(model, sc, SourceParameter(sourceMap("source").toString, sourceType))
    Log.d(TAG, s"Data source is loaded complete")

    //parse raw rdd
    /**
      * public abstract org.apache.spark.rdd.RDD<?> onParseBatchPredictData(
      *   org.apache.spark.SparkContext,
      *   org.apache.spark.rdd.RDD<?>,
      *   BatchPredictDataParameter,
      *   scala.Enumeration$Value);
      */
    val onParseBatchPredictDataMethod = model.getClass.getDeclaredMethod("onParseBatchPredictData",
      classOf[SparkContext],
      classOf[RDD[_]],
      classOf[BatchPredictDataParameter],
      sourceType.getClass.getClassLoader.loadClass("scala.Enumeration$Value"))
    Log.d(TAG, s"onParseBatchPredictData() = ${onParseBatchPredictDataMethod}")

    //data parameter
    dataParameter.parse(dataParameterStr)
    val parsedRdd = onParseBatchPredictDataMethod.invoke(model, sc, rawRdd, dataParameter, sourceType)
    Log.d(TAG, s"Raw RDD is parsed complete")

    //do predict
    /**
      * public abstract void onBatchPredict(
      *   org.apache.spark.SparkContext,
      *   org.apache.spark.rdd.RDD<?>,
      *   java.lang.String);
      */
    val onBatchPredictMethod = model.getClass.getDeclaredMethod("onBatchPredict",
      classOf[SparkContext],
      classOf[RDD[_]],
      classOf[String])
    Log.d(TAG, s"onBatchPredict() = ${onBatchPredictMethod}")
    try{
      onBatchPredictMethod.invoke(model, sc, parsedRdd, resultSavePath)
    } catch {
      case e:InvocationTargetException=>{
        println("========================")
        println(e.getCause)
        println("========================")
      }
    }

    Log.d(TAG, s"Batch predict complete")
  }
}
