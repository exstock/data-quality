package com.hduser.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.{Map => MutableMap}
import java.net.URI

object FSUtils{
  private val fsMap:MutableMap[String,FileSystem]=MutableMap[String,FileSystem]()
  private val defaultFS:FileSystem=FileSystem.get(getConfiguration)

  private def getConfiguration():Configuration={
    val conf=new Configuration()
    conf.setBoolean("dfs.support.append",true)
    conf
  }

  def getUriOpt(path:String):Option[URI]={
    val uriOpt=try{
      Some(new URI(path))
    }catch{
      case e:Throwable=>None
    }
    uriOpt.flatMap{uri=>
      if(uri.getScheme==null){
        try{
          Some(new Path(path).toUri())
        }catch{
          case e:Throwable=>None
        }
      }else{
        Some(uri)
      }
    }

  }
  def getFileSystem(path:String):FileSystem={
    getUriOpt(path) match {
      case Some(uri)=>{
        fsMap.get(uri.getScheme) match{
          case Some(fs)=>fs
          case _=>{
            val fs=try{
              FileSystem.get(uri,getConfiguration())
            }catch{
              case e:Throwable=>{
                println(s"get file system error :${e.getMessage}")
                throw e
              }
            }
            fsMap+=(uri.getScheme->fs)
            fs
          }
        }
    }
    case _=>defaultFS
    }
  }

}
