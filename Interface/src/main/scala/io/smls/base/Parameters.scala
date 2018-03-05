package io.smls.base

import io.smls.base.SourceType.SourceType

/**
  * Source Type
  */
object SourceType extends Enumeration {
  type SourceType = Value
  val TEXT = Value("TEXT")
  val JSON = Value("JSON")
  val CSV = Value("CSV")
  val SEQUENCE = Value("SEQUENCE")
  val OBJECT = Value("OBJECT")
  val HADOOP = Value("HADOOP")
  val WHOLE = Value("WHOLE")
}

/**
  * Data source parameter, define where the data should be loaded from.
  * @param dataSource data source path
  * @param sourceType data source type
  */
final case class SourceParameter(dataSource:String, sourceType:SourceType)

/**
  * Data parameter, define the data's semantics which algorithm can understand.
  * Spark may distribute the class to works, subclass require to be serializable.
  */
trait DataParameter extends Serializable{
  /**
    * parse configuration json of data
    *
    * @param jsonString
    */
  def parse(jsonString:String)
}


/**
  * AlgorithmParameter, define the algorithm's parameter.
  * Spark may distribute the class to works, subclass require to be serializable.
  */
trait AlgorithmParameter extends Serializable{
  /**
    * parse configuration json of algorithm
    *
    * @param jsonString
    */
  def parse(jsonString:String)
}

/**
  * Predict data parameter, define the data's semantics which model can understand.
  * Spark may distribute the class to works, subclass require to be serializable.
  */
trait BatchPredictDataParameter extends Serializable{
  /**
    * parse configuration json of data for predicting
    *
    * @param jsonString
    */
  def parse(jsonString:String)
}