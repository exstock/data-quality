package com.hduser.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object FSUtils {
  private def getConfiguration(): Configuration = {
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    conf
  }
/*

  def getUriOpt(path: String): Option[URI] = {
    val uriOpt = try {
      Some(new URI(path))
    } catch {
      case e: Throwable => None
    }
    uriOpt.flatMap { uri =>
      if (uri.getScheme == null) {
        try {
          Some(new Path(path).toUri())
        } catch {
          case e: Throwable => None
        }
      } else {
        Some(uri)
      }
    }

  }*/


  /**
    * 获取filesystem
    * hdfs://data-warehouse:9000"
    *
    * @param path
    * @return
    */
  def getFileSystem(path: String): FileSystem = {
    val fs = try {
      val uri =new URI(path)
      FileSystem.get(uri, getConfiguration())
    } catch {
      case e: Throwable => {
        println(s"get file system error :${e.getMessage}")
        throw e
      }
    }
    fs
  }
}
