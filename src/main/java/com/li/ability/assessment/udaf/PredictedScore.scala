package com.li.ability.assessment.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}

import scala.collection
import scala.collection.mutable._
import scala.collection.parallel.mutable

class PredictedScore extends UserDefinedAggregateFunction {

  import org.apache.spark.sql.types._

  override def inputSchema: StructType = {

    StructType(Array(
      StructField(" corrects", ArrayType(IntegerType), true),
      StructField(" questions", ArrayType(IntegerType), true),
      StructField(" times", ArrayType(IntegerType), true),
      StructField(" points", ArrayType(IntegerType), true)
    ))
  }

  override def bufferSchema: StructType = {

    StructType(Array(
      StructField("total_station_predict_score", StringType, true),
      StructField("do_exercise_num", ArrayType(IntegerType), true),
      StructField("cumulative_time", LongType, true)
    ))
  }


  override def dataType: DataType = StringType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0, "-1:0:0_-1:0:0")
    buffer.update(1, Array())
    buffer.update(2, 0L)
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val corrects: scala.Seq[Int] = input.getSeq[Int](0)
    val points: scala.Seq[Int] = input.getSeq[Int](3)

    // 记录本次update 知识点下的正确树立
    var mutmap = Map[Int, (Int, Int)]()
    // points.zip(corrects)
    points.zip(corrects)
      .foreach { f =>
        val t = mutmap.getOrElse(f._1, (0, 0))

        if (f._2 == 1) {
          val correct = t._1 + 1
          val total = t._2 + 1
          mutmap += (f._1 -> (correct, total))
        } else {
          val correct = t._1
          val total = t._2 + 1
          mutmap += (f._1 -> (correct, total))
        }
      }
    /**
      * points
      * 本次update的map
      * 642->(24,35)_435->(23,30)_392->(15,20)_482->(10,15)_754->(13,20)
      */
    val inputResult = mutmap
    //      .mkString("_").replaceAll(" ", "")
    // -1:0:0_-1:0:0
    val buf = buffer.getAs[String](0)
    //    val total_st0ation_predict_score: String =
    //    buf.split("\\|").head.split("=")(1)
    var bufferMap = PredictedScore.getTSPredictScore(buf)

    inputResult.foreach { f =>
      val point = f._1
      val correct = f._2._1
      val total = f._2._2

      val buffer_correct = bufferMap.getOrElse(f._1, (0, 0))._1
      val buffer_total = bufferMap.getOrElse(f._1, (0, 0))._2


      bufferMap += (point -> (buffer_correct + correct, buffer_total + total))
    }
    val upd = bufferMap.mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")

    val questions = input.getSeq[Int](1)

    val questionSet = Set[Int]()
    buffer.getAs[Seq[Int]](1).foreach(questionSet += _)

    questions.foreach(f =>
      questionSet += f
    )

    val times = input.getSeq[Int](2)
    var timeArr = buffer.getAs[Long](2)
    times.foreach(f =>
      timeArr += timeArr
    )

    buffer.update(
      0, upd
    )


    buffer.update(1, questionSet.toSeq)
    buffer.update(2, timeArr)
  }


  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val buffer1Map = PredictedScore.getTSPredictScore(buffer1.getAs[String](0))

    val buffer2Map = PredictedScore.getTSPredictScore(buffer2.getAs[String](0))

    val result = PredictedScore.mergeMap(buffer1Map, buffer2Map).mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")
    buffer1.update(0, result)

    // 本次
    val questions = buffer2.getSeq[Int](1)
    // 合并
    val questionSet = Set[Int]()
    buffer1.getAs[Seq[Int]](1).foreach(questionSet += _)

    questions.foreach(f =>
      questionSet += f
    )
    buffer1.update(1, questionSet.toSeq)


    val times = buffer2.getLong(2)
    var timeArr = buffer1.getLong(2)
    buffer1.update(2, times + timeArr)
  }

  override def evaluate(buffer: Row): Any = {

    val total_station_predict_score = buffer.getAs[String](0)
    val do_exercise_num = buffer.getAs[collection.mutable.Set[Int]](1).size
    val cumulative_time = buffer.getAs[Long](2)

    total_station_predict_score.concat("|").concat(do_exercise_num.toString).concat("|").concat(cumulative_time.toString)

  }


}

object PredictedScore {
  /**
    * -1:0:0_-1:0:0
    */
  def getTSPredictScore(str: String): scala.collection.mutable.Map[Int, (Int, Int)] = {

    var mutmap = Map[Int, (Int, Int)]()
    val total_station_predict_score = str

    total_station_predict_score.split("_").foreach(f => {
      val arr = f.split(":")
      mutmap += (arr(0).toInt -> (arr(1).toInt, arr(2).toInt))
    })

    mutmap
  }

  def mergeMap(in: Map[Int, (Int, Int)], buffer: Map[Int, (Int, Int)]): Map[Int, (Int, Int)] = {

    in.foreach { f =>
      val point = f._1
      val correct = f._2._1
      val total = f._2._2

      val count = buffer.getOrElse(f._1, (0, 0))
      buffer += (point -> (count._1 + correct, count._2 + total))
    }
    buffer
  }

  def main(args: Array[String]): Unit = {

    //      val str = getTSPredictScore("total_station_predict_score=-1:0:0_-1:0:0|")
    //  println(str)
    var jarSet = Set("Tom", "Jerry")
    jarSet += "Tom"
    println(jarSet)
  }

  //  def main(args: Array[String]): Unit = {
  //
  //    val a = Seq(1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 2, 1, 1, 2, 2, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 1, 2, 1, 2, 2, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1)
  //    val b = Seq(642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 642, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 754, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 482, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 435, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392, 392)
  //
  //    var mumap = Map[Int, (Int, Int)]()
  //
  //
  //    b.zip(a)
  //      .foreach { f =>
  //        val t = mumap.getOrElse(f._1, (0, 0))
  //
  //        if (f._2 == 1) {
  //          val correct = t._1 + 1
  //          val total = t._2 + 1
  //          mumap += (f._1 -> (correct, total))
  //        } else {
  //          val correct = t._1
  //          val total = t._2 + 1
  //          mumap += (f._1 -> (correct, total))
  //        }
  //      }
  //
  //    println(mumap.mkString("_")
  //      .replaceAll(" ", "")
  //      .replaceAll("->\\(", ":")
  //      .replaceAll("\\)", "")
  //      .replaceAll(",", ":"))
  //  }
}
