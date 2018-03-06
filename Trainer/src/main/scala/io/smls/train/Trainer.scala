package io.smls.train

import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}

import io.smls.base._
import io.smls.base.util.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Runnable application of ml training.
  *
  * This application take arguments as below:
  * {{{
  * --------------------------spark configuration------------------------------
  * [mandatory] -appName:String                  name of the application
  *
  * --------------------------runtime configuration----------------------------
  * [mandatory] -algoPluginPath:String           path of the plugin jar
  * [mandatory] -algoClass:String                class name of algorithm
  * [mandatory] -dataParameterClass:String       class name of data parameter
  * [mandatory] -algoParameterClass:String       class name of algorithm parameter
  *
  * --------------------------algorithm configuration--------------------------
  * [mandatory] -sourceParameter:JSONString      description of data source
  *              {
  *                "source":source[String],      source from
  *                "type":type[String]           source type, same as SourceType class
  *              }
  * [mandatory] -dataParameter:JSONString        description of data
  *                                              the format should be specified by algorithm plugin
  *
  * [mandatory] -algoParameter:JSONString        description of algorithm
  *                                              the format should be specified by algorithm plugin
  *
  * [optional]  -dataWeight:JSONString           description of the training and verifying data weight
  *              {
  *                "dataWeight":[Double, Double] ratio of training data and cross verify data,
  *              }                               default is 1.0,1.0
  *
  * [optional]  -modelSavePath:String            path for saving the model,
  *                                              default is null
  * [optional]  -modelIndicatorSavePath:String   path for saving the model's indicator,
  *                                              default is null
  * }}}
  */
object Trainer {
  private[this] def parseArgument(args:Array[String]):Map[String, _] = {
    //all the arguments must be set and no NULL value
    def nextOption(map : Map[String, _], list: List[String]) : Map[String, _] = {
      list match {
        case Nil => map
        /*case "-master" :: value :: tail =>
          nextOption(map ++ Map("master" -> value), tail)*/
        case "-appName" :: value :: tail =>
          nextOption(map ++ Map("appName" -> value), tail)
        case "-algoPluginPath" :: value :: tail =>
          nextOption(map ++ Map("algoPluginPath" -> value), tail)
        case "-algoClass" :: value :: tail =>
          nextOption(map ++ Map("algoClass" -> value), tail)
        case "-dataParameterClass" :: value :: tail =>
          nextOption(map ++ Map("dataParameterClass" -> value), tail)
        case "-algoParameterClass" :: value :: tail =>
          nextOption(map ++ Map("algoParameterClass" -> value), tail)
        case "-sourceParameter" :: value :: tail =>
          nextOption(map ++ Map("sourceParameter" -> value), tail)
        case "-dataParameter" :: value :: tail =>
          nextOption(map ++ Map("dataParameter" -> value), tail)
        case "-algoParameter" :: value :: tail =>
          nextOption(map ++ Map("algoParameter" -> value), tail)
        case "-dataWeight" :: value :: tail =>
          nextOption(map ++ Map("dataWeight" -> value), tail)
        case "-modelSavePath" :: value :: tail =>
          nextOption(map ++ Map("modelSavePath" -> value), tail)
        case "-modelIndicatorSavePath" :: value :: tail =>
          nextOption(map ++ Map("modelIndicatorSavePath" -> value), tail)
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

    //spark configuration
    //val masterAddress  = argMap("master").toString
    val appName = argMap("algoClass").toString.split("\\.").last +":"+argMap("appName").toString
    val conf = new SparkConf()
    conf/*.setMaster(masterAddress)*/.setAppName(appName)
    val sc = new SparkContext(conf)
    Log.d(TAG, s"Trainer start to run application ${appName} (app id = ${sc.applicationId})")

    //load class from jar
    val classLoader = new URLClassLoader(Array[URL](new File(argMap("algoPluginPath").toString).toURI.toURL),
      this.getClass.getClassLoader)
    val algorithm = classLoader.loadClass(argMap("algoClass").toString).newInstance()/*.asInstanceOf[AbstractAlgorithm]*/
    val dataParameter = classLoader.loadClass(argMap("dataParameterClass").toString).newInstance().asInstanceOf[DataParameter]
    val algoParameter = classLoader.loadClass(argMap("algoParameterClass").toString).newInstance().asInstanceOf[AlgorithmParameter]

    //use scala JSON parsing lib, keep the jar minimum size
    //source parameter
    val sourceMap = JSON.parseFull(argMap("sourceParameter").toString).get.asInstanceOf[Map[String, _]]
    val typeArg = sourceMap("type").toString.toUpperCase
    if(!SourceType.values.map(_.toString).contains(typeArg)){
      throw new IllegalArgumentException("Unsupported source type:" + typeArg)
    }
    val sourceType = SourceType.withName(typeArg)

    //data parameter
    dataParameter.parse(argMap("dataParameter").toString)

    //algorithm parameter
    algoParameter.parse(argMap("algoParameter").toString)

    //optional parameters
    val modePath = argMap.getOrElse("modelSavePath", null)
    val indicatorPath = argMap.getOrElse("modelIndicatorSavePath", null)
    var weight:(Double, Double) = (1.0, 0)

    val weightArg = argMap.getOrElse("dataWeight", null)
    if(weightArg != null){
      val weightMap = JSON.parseFull(weightArg.toString).get.asInstanceOf[Map[String, _]]
      val weightList = weightMap("dataWeight").asInstanceOf[List[Double]]
      if (weightList.size >= 2){
        val uncheckedWeight = (weightList(0), weightList(1))
        if (uncheckedWeight._1 <= 0 || uncheckedWeight._1 >1 ||
          uncheckedWeight._2 < 0 || uncheckedWeight._2 >1 ||
          uncheckedWeight._1 + uncheckedWeight._2 > 1){
          throw new IllegalArgumentException(s"Invalid weight:${weight}")
        }
        weight = uncheckedWeight
      }
    }


    //---------------------------------------------------------------------------------------------------//
    //-------------------------------------------Unsafe Code---------------------------------------------//
    //---------------------------------------------------------------------------------------------------//
    //load raw rdd: algorithm.onLoad()
    /** public org.apache.spark.rdd.RDD<?> onLoad(
      *     org.apache.spark.SparkContext,
      *     io.smls.base.SourceParameter)*/
    val onLoadMethod = algorithm.getClass.getDeclaredMethod("onLoad", sc.getClass, classOf[SourceParameter])
    Log.d(TAG, s"onLoad() = ${onLoadMethod}")
    val rawRdd = onLoadMethod.invoke(algorithm,
      sc,
      SourceParameter(sourceMap("source").toString, sourceType))
    Log.d(TAG, "Data source is loaded complete")

    //parse raw rdd: algorithm.onParse()
    /**  public org.apache.spark.rdd.RDD onParse(
      *     org.apache.spark.SparkContext,
      *     org.apache.spark.rdd.RDD,
      *     io.smls.base.DataParameter,
      *     scala.Enumeration$Value);
      */
    val onParseMethod = algorithm.getClass.getDeclaredMethod("onParse",
      classOf[SparkContext],
      classOf[RDD[_]],
      classOf[DataParameter],
      sourceType.getClass.getClassLoader.loadClass("scala.Enumeration$Value"))
    Log.d(TAG, s"onParse() = ${onParseMethod}")
    var Array(trainRdd, cvRdd) =
      onParseMethod.invoke(algorithm,
        sc,
        rawRdd,
        dataParameter,
        sourceType).asInstanceOf[RDD[_]].randomSplit(Array(weight._1, weight._2))
    Log.d(TAG, "Raw RDD is parsed complete")

    //training model
    Log.d(TAG, s"Start training, train ratio:${weight._1} / verify ratio:${weight._2}")
    /**public java.lang.Object onTrain(
      *     org.apache.spark.SparkContext,
      *     org.apache.spark.rdd.RDD,
      *     io.smls.base.AlgorithmParameter);*/
    val onTrainMethod = algorithm.getClass.getDeclaredMethod("onTrain",
      classOf[SparkContext],
      classOf[RDD[_]],
      classOf[AlgorithmParameter])
    Log.d(TAG, s"onTrain() = ${onTrainMethod}")
    val model = onTrainMethod.invoke(algorithm, sc, trainRdd, algoParameter)
    Log.d(TAG, s"Training complete, model: ${model.getClass}")


    //verifying model
    /**public java.lang.String onVerify(
      *     org.apache.spark.SparkContext,
      *     org.apache.spark.rdd.RDD,
      *     java.lang.Object);*/
    if(weight._2 == 0){ //no verify data, use the training data to verify
      cvRdd = trainRdd
    }
    val onVerifyMethod = algorithm.getClass.getDeclaredMethod("onVerify",
      classOf[SparkContext],
      classOf[RDD[_]],
      classOf[Object])
    Log.d(TAG, s"onVerify() = ${onVerifyMethod}")
    val modelIndicatorResult = onVerifyMethod.invoke(algorithm, sc, cvRdd, model)
    Log.d(TAG, s"Verify complete, result: ${modelIndicatorResult}")

    //saving model and indicator
    if(modePath != null){
      /**public void save(
        *   org.apache.spark.SparkContext,
        *   java.lang.String);*/
      val saveMethod = model.getClass.getDeclaredMethod("save",
        classOf[SparkContext],
        classOf[String])
      Log.d(TAG, s"save() = ${saveMethod}")
      saveMethod.invoke(model, sc, modePath.toString)
      Log.d(TAG, s"Save model complete, path: ${modePath}")
    } else {
      Log.d(TAG, "No mode saving path is provided, saving nothing")
    }

    if(indicatorPath != null){
      val indicatorPathStr = indicatorPath.toString
      if (indicatorPathStr.startsWith("hdfs")) {
        HdfsUtil.write(indicatorPathStr, modelIndicatorResult.toString)
      } else {
        //local fs
        val writer = new PrintWriter(new File(indicatorPathStr))
        writer.write(modelIndicatorResult.toString)
        writer.flush()
        writer.close()
      }

      Log.d(TAG, s"Save model indicator complete, path: ${indicatorPath}")
    } else {
      Log.d(TAG, "No mode indicator saving path is provided, saving nothing")
    }

    Log.d(TAG, "end train()")
  }
}