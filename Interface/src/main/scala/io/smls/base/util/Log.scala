package io.smls.base.util

/**
  * Log util
  */
object Log {
  def d(tag:String, msg:String):Unit = {
    println(s"INFO [${tag}]\t${msg}")
  }

  def w(tag:String, msg:String):Unit = {
    println(s"WARN [${tag}]\t${msg}")
  }

  def e(tag:String, msg:String):Unit = {
    println(s"ERRO [${tag}]\t${msg}")
  }
}
