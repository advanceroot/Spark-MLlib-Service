package io.smls.base

import io.smls.base.SourceType.SourceType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Model for predicting.
  */
trait AbstractModel[DataParam<:BatchPredictDataParameter, ParsedRDD]{
  /**
    * Name of model
    */
  val name:String


  /**
    * Callback of loading model.
    *
    * Prepare all the resources for predicting.
    *
    * @param sc SparkContext
    * @param modelPath Model saving path or any resource's path.
    */
  def onLoadModel(sc:SparkContext, modelPath:String):Unit

  /**
    * Callback of single predicting.
    *
    * Do predict according your model.
    * The input type is String which means it could be anything.Implementation should
    * provide a specification to let user to obey.Json format is preferred.
    * The result type is String also. Just make sure your client understand the result
    * and can parse from it. Json format is preferred.
    *
    * @param sc SparkContext
    * @param input data json
    * @return Result predict result
    */
  def onPredict(sc:SparkContext, input:String):String


  /**
    * Callback of loading raw RDD from data source for batch predicting.
    *
    * @param sc SparkContext
    * @param source SourceParameter
    * @return RDD[Raw type]
    */
  def onLoadBatchPredictData(sc:SparkContext, source:SourceParameter): RDD[_] = {
    var rdd:RDD[_] = null

    source.sourceType match {
      case SourceType.TEXT => rdd = sc.textFile(source.dataSource)      //RDD[String]
      case SourceType.HADOOP => rdd = sc.hadoopFile(source.dataSource)  //RDD[(K,V)]
      case SourceType.OBJECT => rdd = sc.objectFile(source.dataSource)  //RDD[T]
      case SourceType.SEQUENCE => rdd = sc.sequenceFile(source.dataSource, null, null) //RDD[(K,V)]
      case SourceType.JSON => rdd = sc.wholeTextFiles(source.dataSource) //RDD[(String, String)]
      case _ => throw new IllegalArgumentException(s"Unsupported data type:${source.sourceType}")
    }

    rdd
  }

  /**
    * Callback of preparing RDD for batch predicting.
    * Implement this method to build RDD which Model can deal with.
    *
    * @param sc SparkContext
    * @param rawRDD RDD[_] the raw type of RDD
    * @param parameter DataParam the data parameter about this predicting
    * @param sourceType SourceType
    * @return RDD[ParsedRDD] the parsed RDD
    */
  def onParseBatchPredictData(sc:SparkContext,
              rawRDD:RDD[_],
              parameter:DataParam,
              sourceType:SourceType):RDD[ParsedRDD]

  /**
    * Callback of batch predicting.
    * Implementation should use model to predict the input RDD,
    * and save result to file set by savePath parameter.
    * The content type of output file should be specified by
    * plugin's implementation.
    *
    * @param sc SparkContext
    * @param rdd RDD[ParsedRDD] the parsed RDD
    * @param savePath String the result file path
    */
  def onBatchPredict(sc:SparkContext, rdd:RDD[ParsedRDD], savePath:String):Unit


  /**
    * Callback of unloading model.
    *
    * Release resources if any.
    *
    * @param sc SparkContext
    */
  def onUnload(sc:SparkContext):Unit
}
