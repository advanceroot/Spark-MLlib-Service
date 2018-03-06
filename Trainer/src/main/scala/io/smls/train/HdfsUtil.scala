package io.smls.train

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * Utils of Hdfs writing back.
  */
object HdfsUtil {
  def write(hdfsPath:String, content:String):Unit = {
    val parsed = parseHdfsPath(hdfsPath)
    write(parsed._1, parsed._2, content)
  }

  def write(hdfsPath:String, content:Array[Byte]):Unit = {
    val parsed = parseHdfsPath(hdfsPath)
    write(parsed._1, parsed._2, content)
  }

  def delete(hdfsPath:String):Unit = {
    val parsed = parseHdfsPath(hdfsPath)
    delete(parsed._1, parsed._2)
  }

  def delete(hdfsUri:String, path:String):Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)
    fs.delete(new Path(path), true)
  }

  def write(hdfsUri:String, path:String, content:String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)
    val fos = fs.create(new Path(path))
    val writer = new PrintWriter(fos)
    writer.write(content)
    writer.flush()
    writer.close()
    fos.close()
  }

  def write(hdfsUri:String, path:String, content:Array[Byte]) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)
    val fos = fs.create(new Path(path))
    fos.write(content)
    fos.flush()
    fos.close()
  }

  private def parseHdfsPath(hdfsPath:String):(String, String) = {
    if (!hdfsPath.startsWith("hdfs://")){
      throw new IllegalArgumentException(s"hdfs path should start at hdfs:// (${hdfsPath})")
    }

    val portIndex = hdfsPath.indexOf(":", 5)
    if(portIndex == -1){
      throw new IllegalArgumentException(s"hdfs path should have port (${hdfsPath})")
    }

    val pathStart = hdfsPath.indexOf("/", portIndex)
    if(pathStart == -1){
      throw new IllegalArgumentException(s"hdfs path should have path (${hdfsPath})")
    }

    (hdfsPath.substring(0, pathStart), hdfsPath.substring(pathStart))
  }
}
