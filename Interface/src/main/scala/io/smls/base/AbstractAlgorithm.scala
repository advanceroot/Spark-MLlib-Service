package io.smls.base

import io.smls.base.SourceType.SourceType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  * Algorithm running at Spark.
  *
  * @tparam DataParam DataParameter
  * @tparam AlgoParam AlgorithmParameter
  * @tparam ParsedRDD RDD the algorithm used
  * @tparam Model Model the algorithm trained, a save method must be required in Model
  */
trait AbstractAlgorithm[DataParam<:DataParameter,
                        AlgoParam<:AlgorithmParameter,
                        ParsedRDD,
                        Model <: {
                          def save(sc: SparkContext, path: String): Unit
                        }]
{
  /**
    * Name of algorithm.
    */
  val name:String

  /**
    * Callback of loading raw RDD from data source.
    *
    * @param sc SparkContext
    * @param source SourceParameter
    * @return RDD[_] the type of RDD may be different according the type of source
    */
  def onLoad(sc:SparkContext,
             source:SourceParameter): RDD[_] = {
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
    * Callback of preparing RDD for training.
    * Implement this method to build RDD which Algorithm can deal with.
    * For example, build RDD of Rating in Recommend Algorithm, build RDD of
    * LabeledPoint in DecisionTree Algorithm etc.
    *
    * @param sc SparkContext
    * @param rawRDD RDD[_] the raw type of RDD
    * @param parameter DataParam the data parameter about this algorithm
    * @param sourceType SourceType
    * @return RDD[ParsedRDD] the parsed RDD
    */
  def onParse(sc:SparkContext,
              rawRDD:RDD[_],
              parameter:DataParam,
              sourceType:SourceType):RDD[ParsedRDD]



  /**
    *Callback of training model.
    *
    * @param sc SparkContext
    * @param trainRDD RDD[ParsedRDD] which is returned by onParse()
    * @param param AlgoParam the parameter of this algorithm
    * @return Model training result
    */
  def onTrain(sc:SparkContext,
              trainRDD:RDD[ParsedRDD],
              param:AlgoParam):Model


  /**
    * Callback of verifying model.
    *
    * @param sc SparkContext
    * @param verifyRDD RDD[ParsedRDD] to be verified on
    * @param model Model
    * @return String indicator of the model, the format should be readable for client, JSON is preferred
    */
  def onVerify(sc:SparkContext,
               verifyRDD:RDD[ParsedRDD],
               model:Model):String
}