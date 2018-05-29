package com.github.scala.wordcount

import scala.actors.{Actor, Future}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * 用actor并发编程写一个单机版的WordCount，
  * 将多个文件作为输入，计算完成后将多个任务汇总，得到最终的结果
  */

// 将文件名传入CountTask中，读取数据
case class SubmitTask(filename:String)
// 将得到的结果从actor返回
case class ResultTask(result: Map[String, Int])

class CountTask extends Actor {
  override def act(): Unit = {
    loop{
      react{
        case SubmitTask(filename) => {
          // 1.利用source读取文件内容
          val fileContent: String = Source.fromFile(filename).mkString

          // 2.按照换行符切分，window下文件的换行符 \r\n, linux下文件的换行符 \n
          val lines = fileContent.split("\r\n")

          // 3.切分每一行，获得每个单词 flatMap = flatten + map
          val words: Array[String] = lines.flatMap(_.split(" "))

          // 4.每个单词个数记为1
          val markedWord: Array[(String, Int)] = words.map((_, 1)) //map(x=>(x,1))

          // 5.相同的单词进行分组
          val wordGroup: Map[String, Array[(String, Int)]] = markedWord.groupBy(_._1)

          // 6.统计每个单词出现的次数
          val result: Map[String, Int] = wordGroup.mapValues(_.length)

          // 7.将结果返回给发送方 !异步无返回
          sender ! ResultTask(result)
        }
      }
    }
  }
}

object WordCount extends App {
  // 定义一个保存Future的set集合
  private val resultSet: mutable.HashSet[Future[Any]] = new mutable.HashSet[Future[Any]]
  // 存储由CountTask返回的数据
  private val tasks: ListBuffer[ResultTask] = new ListBuffer[ResultTask]

  // 准备数据文件
  val files = Array("E:\\Code\\scala\\wordcount\\1.txt", "E:\\Code\\scala\\wordcount\\2.txt", "E:\\Code\\scala\\wordcount\\3.txt")
  // 遍历数据文件，提交任务
  for (file <- files) {
    // 创建actor
    val countTask = new CountTask
    // 启动actor
    countTask.start()
    // 发送文件名 !!发送异步消息，返回值是 Future[Any]
    val result: Future[Any] = countTask !! SubmitTask(file)
    // 添加到存储集合
    resultSet += result
  }

  // 遍历结果集合，将不同文件的单词个数聚合
  while (resultSet.nonEmpty) {
    // 过滤出执行完毕，有结果数据的Future
    val completedFuture: mutable.HashSet[Future[Any]] = resultSet.filter(_.isSet)
    // 遍历执行完毕的Future，得到数据
    for (t <- completedFuture) {
      // 获取数据
      val apply: Any = t.apply()
      // 添加数据到list集合中
      tasks += apply.asInstanceOf[ResultTask]
      // 在resultSet中移除执行完毕的数据
      resultSet -= t
    }
  }

  private val wordsNum: Map[String, Int] = tasks.map(_.result).flatten.groupBy(_._1).mapValues(x=>x.foldLeft(0)(_+_._2))
  println(wordsNum)

}
