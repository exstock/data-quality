package com.hduser.utils

import scala.collection.Iterable
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object HdfsUtils{
  private val separate="/"

  private def getFs(implicit path:Path):FileSystem=FSUtils.getFileSystem(path.toString)

  /**
    * 判断给定的文件目录是否在
    * @param filePath
    * @return
    */
  def existsPath(filePath:String):Boolean={
    try{
      implicit val path=new Path(filePath)
      getFs.exists(path)
    }
    catch{
      case e:Throwable=>false
    }
  }

  /**
    * 获取路径
    * @param parentPath
    * @param fileName
    * @return
    */
  def getHdfsFilePath(parentPath:String,fileName:String):String={
    if (parentPath.endsWith(separate)){
      parentPath+fileName
    }
    else{
      parentPath+separate+fileName
    }

  }

  /**
    * 判断文件是否在给定目录中存在
    * @param dirPath
    * @param fileName
    * @return
    */
  def existsFileInDir(dirPath:String,fileName:String):Boolean={
    val filePath=getHdfsFilePath(dirPath,fileName)
    existsPath(filePath)
  }

  /**
    * 创建文件
    * @param filePath
    * @return
    */
  def createFile(filePath:String):FSDataOutputStream={
    implicit val path=new Path(filePath)
    if (getFs.exists(path)){
      getFs.delete(path,true)
    }
    val fsDataOutputStream:FSDataOutputStream=getFs.create(path)
    fsDataOutputStream
  }

  /**
    * 追加或者创建文件
    * @param filePath
    * @return
    */
  def appendOrCreateFile(filePath:String):FSDataOutputStream={
    implicit val path=new Path(filePath)
    if(getFs.getConf.getBoolean("dfs.support.append", false)&&getFs.exists(path)){
      getFs.append(path)
    }
    else
      createFile(filePath)
  }

  /**
    * 打开文件
    * @param filePath
    * @return
    */
  def openFile(filePath:String):FSDataInputStream={
    implicit val path=new Path(filePath)
    getFs.open(path)
  }

  /**
    * 写入内容
    * @param filepath
    * @param msg
    */
  def writeContent(filepath:String,msg:String):Unit={
    val out =createFile(filepath)
    out.write(msg.getBytes("utf-8"))
    out.close()
  }

  /**
    * 写入内容
    * @param filePath
    * @param msg
    */
  def appendContent(filePath:String,msg:String):Unit={
    val out =appendOrCreateFile(filePath)
    out.write(msg.getBytes("utf-8"))
    out.close()
  }

  /**
    * 创建空文件
    * @param filePath
    */
  def createEmptyFile(filePath:String):Unit={
    val out =createFile(filePath)
    out.close()
  }

  /**
    * 删除路径
    * @param filePath
    */
 def deleteHdfsPath(filePath:String):Unit={
   try{
     implicit val path=new Path(filePath)
     if (getFs.exists(path)){
       getFs.delete(path,true)
     }
   }catch{
     case e:Throwable=>{
       println(s"delete path ${filePath} error:${e.getMessage}")
     }
   }
 }

  /**
    * 路径名称
    * @param filePath
    * @return
    */
  def fileNameFromPath(filePath:String):String={
    val path=new Path(filePath)
    path.getName
  }

  /**
    * 根据给定的文件类型获取指定目录下的文件名
    * @param dirPath
    * @param subType
    * @param fullPath
    * @return
    */
  def listSubPathsByType(dirPath:String,subType:String,fullPath:Boolean=false):Iterable[String]={
    if (existsPath(dirPath)){
      try{
        implicit val path=new Path(dirPath)
        val fileStatusArray=getFs.listStatus(path)
        fileStatusArray.filter{
          fileStatus=>{
            subType match{
              case "dir"=>fileStatus.isDirectory
              case "file"=>fileStatus.isFile
              case _=>true
            }
          }
        }.map{
          fileStatus=>{
            val fileName=fileStatus.getPath.getName
            if (fullPath){
              getHdfsFilePath(dirPath,fileName)
            }
            else{
              fileName
            }
          }
        }
      }catch{
        case e:Throwable=>{
          println(s"list path ${dirPath} warn:${e.getMessage}")
          Nil
        }
      }
    }
    else Nil
  }

  /**
    * 给定多个文件类型，获取指定目录下的文件名
    * @param dirPath
    * @param subTypes
    * @param fullPath
    * @return
    */
  def listSubPathsByTypes(dirPath:String,subTypes:Iterable[String],fullPath:Boolean=false):Iterable[String]={
    subTypes.flatMap(subType=>listSubPathsByType(dirPath,subType,fullPath))
  }
}
