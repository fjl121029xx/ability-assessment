package com.li.ability.assessment.udaf

import java.text.SimpleDateFormat

import com.li.ability.assessment.TimeUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}

import scala.collection.mutable._

class PredictedScore extends UserDefinedAggregateFunction {

  import org.apache.spark.sql.types._


  override def inputSchema: StructType = {

    StructType(Array(
      StructField("corrects", ArrayType(IntegerType), true),
      StructField("questions", ArrayType(IntegerType), true),
      StructField("times", ArrayType(IntegerType), true),
      StructField("points", ArrayType(IntegerType), true),
      StructField("createTime", StringType, true)
    ))
  }

  override def bufferSchema: StructType = {

    StructType(Array(
      StructField("total_station_predict_score", StringType, true),
      StructField("do_exercise_num", ArrayType(IntegerType), true),
      StructField("cumulative_time", IntegerType, true),
      StructField("week_predict_score", StringType, true),
      StructField("createTime", LongType, true),
      StructField("do_exercise_day", ArrayType(StringType), true),
      StructField("week_do_exercise_num", ArrayType(IntegerType), true),
      StructField("week_cumulative_time", IntegerType, true),
      StructField("total_correct_num", LongType, true),
      StructField("week_correct_num", LongType, true),
      StructField("total_undo_num", LongType, true),
      StructField("week_undo_num", LongType, true)

    ))
  }


  override def dataType: DataType = ArrayType(StringType)


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    // total_station_predict_score
    buffer.update(0, "-1:0:0:0:0_-1:0:0:0:0")
    // do_exercise_num
    buffer.update(1, Array())
    // cumulative_time
    buffer.update(2, 0)
    // week_predict_score
    buffer.update(3, "-1:0:0:0:0_-1:0:0:0:0")
    // createTime
    buffer.update(4, 0L)
    // do_exercise_day
    buffer.update(5, Array())
    // week_do_exercise_num
    buffer.update(6, Array())
    // week_cumulative_time
    buffer.update(7, 0)
    // total_correct_num
    buffer.update(8, 0L)
    // week_correct_num
    buffer.update(9, 0L)
    // total_undo_num
    buffer.update(10, 0L)
    // week_undo_num
    buffer.update(11, 0L)

  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {


    val corrects: scala.Seq[Int] = input.getSeq[Int](0)
    val points: scala.Seq[Int] = input.getSeq[Int](3)
    val times: scala.Seq[Int] = input.getSeq[Int](2)


    var total_correct_num = buffer.get(8).asInstanceOf[Long].longValue()
    var total_undo_num = buffer.get(10).asInstanceOf[Long].longValue()


    // 记录本次update 知识点下的正确树立
    var mutmap = Map[Int, (Int, Int, Int, Int)]()
    // points.zip(corrects)
    points.zip(corrects).zip(times)
      .foreach { f =>
        val t = mutmap.getOrElse(f._1._1, (0, 0, 0, 0))

        if (f._1._2 == 1) {
          val correct = t._1 + 1
          val total = t._2 + 1
          val tim = t._3 + f._2
          val undo = t._4

          mutmap += (f._1._1 -> (correct, total, tim, undo))

        }
//
        else if (f._1._2 == 2){
          val correct = t._1
          val total = t._2 + 1
          val tim = t._3 + f._2
          val undo = t._4
          mutmap += (f._1._1 -> (correct, total, tim, undo))
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
    var bufferMap = PredictedScore.getTSPredictScore2Map(buf)

    inputResult.foreach { f =>
      val point = f._1
      val correct = f._2._1
      val total = f._2._2
      val time = f._2._3
      val undo = f._2._4

      val buffer_correct = bufferMap.getOrElse(f._1, (0, 0, 0, 0))._1
      val buffer_total = bufferMap.getOrElse(f._1, (0, 0, 0, 0))._2
      val buffer_time = bufferMap.getOrElse(f._1, (0, 0, 0, 0))._3
      val buffer_undo = bufferMap.getOrElse(f._1, (0, 0, 0, 0))._4

      total_correct_num += buffer_correct + correct
      total_undo_num += buffer_undo + undo

      bufferMap += (point -> (buffer_correct + correct, buffer_total + total, buffer_time + time, buffer_undo + undo))
    }
    val upd = bufferMap.mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")
    //total_station_predict_score
    buffer.update(
      0, upd
    )
    buffer.update(8, total_correct_num)
    buffer.update(10, total_undo_num)

    // 做题数量
    val questions = input.getSeq[Int](1)

    val quesArr = new ArrayBuffer[Int]()
    buffer.getAs[Seq[Int]](1).foreach(quesArr += _)
    questions.zip(corrects).foreach{
      case (a,b) =>{
        if (b == 1 ||b==2){
          quesArr += a
        }
      }
    }

    //do_exercise_num
    buffer.update(1, quesArr)

    // 获得输入的做题时间
    var cumulativeTime = buffer.get(2).asInstanceOf[Int].intValue()
    input.getSeq[Int](2).toArray.foreach(f =>
      cumulativeTime += f.intValue()
    )
    //cumulative_time
    buffer.update(2, cumulativeTime)
    val format = new SimpleDateFormat("yyyyMMdd")
    //createTimeHBaseUtil
    val createTime = input.get(4).getClass.getName match {
      case "java.lang.Integer" => input.get(4).asInstanceOf[Int].intValue()
      case "java.lang.Long" => input.get(4).asInstanceOf[Long].longValue()
      case "java.lang.String" => {

        val createTime = input.get(4).asInstanceOf[String].toString
        //        println(createTime)
        format.parse(createTime).getTime
      }
    }

    val week_start: Long = TimeUtils.getWeekStartTimeStamp()
    val week_end: Long = TimeUtils.getWeekEndTimeStamp()

    var week_correct_num = buffer.get(9).asInstanceOf[Long].longValue()
    var week_undo_num = buffer.get(11).asInstanceOf[Long].longValue()
    if (week_start <= createTime && createTime < week_end) {

      // mutmap 是本次输入

      val week_buffer_map = PredictedScore.getTSPredictScore2Map(buffer.getAs[String](3))

      mutmap.foreach { f =>
        val point = f._1
        val correct = f._2._1
        val total = f._2._2
        val tim = f._2._3
        val undo = f._2._4

        val buffer_correct = week_buffer_map.getOrElse(f._1, (0, 0, 0, 0))._1
        val buffer_total = week_buffer_map.getOrElse(f._1, (0, 0, 0, 0))._2
        val buffer_time = week_buffer_map.getOrElse(f._1, (0, 0, 0, 0))._3
        val buffer_undo = week_buffer_map.getOrElse(f._1, (0, 0, 0, 0))._4

        week_buffer_map += (point -> (buffer_correct + correct, buffer_total + total, buffer_time + tim, buffer_undo + undo))

        week_correct_num += buffer_correct + correct
        week_undo_num += buffer_undo + undo
      }

      val week_predicted_score = week_buffer_map.mkString("_")
        .replaceAll(" ", "")
        .replaceAll("->\\(", ":")
        .replaceAll("\\)", "")
        .replaceAll(",", ":")

      //      val week_predicted_score = PredictedScore.weekPredictedScore(mutmap, PredictedScore.getTSPredictScore2Map(buffer.getAs[String](3)))
      //week_predict_score
      buffer.update(3, week_predicted_score)


      buffer.update(9, week_correct_num)
      buffer.update(11, week_undo_num)

      val weekQuesArr = new ArrayBuffer[Int]()
      buffer.getAs[Seq[Int]](6).foreach(weekQuesArr += _)

      val questions = input.getSeq[Int](1)

      questions.zip(corrects).foreach{
        case (a,b) =>{
          if (b == 1 ||b==2){
            weekQuesArr += a
          }
        }
      }

      buffer.update(6, weekQuesArr.toSeq)


      var weekCumulativeTime = buffer.get(7).asInstanceOf[Int].intValue()

      input.getSeq[Int](2).toArray.foreach(f =>
        weekCumulativeTime += f.intValue()
      )
      //cumulative_time
      buffer.update(7, weekCumulativeTime)

    }
    else {
      buffer.update(3, buffer.getAs[String](3))
      buffer.update(6, buffer.getAs[Seq[Int]](6))
      buffer.update(7, buffer.getAs[Int](7))
      buffer.update(9, buffer.getAs[Long](9))
      buffer.update(11, buffer.getAs[Long](11))
    }

    val daySet = Set[String]()
    buffer.getAs[Seq[String]](5).foreach(daySet += _)
    daySet += TimeUtils.convertTimeStamp2DateStr(createTime, "yyyy-MM-dd")
    //do_exercise_day
    buffer.update(5, daySet.toSeq)
  }


  override def merge(aggreBuffer: MutableAggregationBuffer, row: Row): Unit = {

    //total_station_predict_score
    val total_station_predicted_score = PredictedScore.mergeMap(aggreBuffer.getAs[String](0), row.getAs[String](0))
      .mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")

    aggreBuffer.update(0, total_station_predicted_score)

    val questions = row.get(1).asInstanceOf[Seq[Int]].seq
    val quesArr = new ArrayBuffer[Int]()
    aggreBuffer.get(1).asInstanceOf[Seq[Int]].seq.foreach(quesArr += _)
    questions.foreach(f =>
      quesArr += f
    )
    //do_exercise_num
    aggreBuffer.update(1, quesArr.toSeq)
    //cumulative_time
    aggreBuffer.update(2, row.get(2).asInstanceOf[Int].intValue() + aggreBuffer.get(2).asInstanceOf[Int].intValue())


    val week_predicted_score = PredictedScore.mergeMap(aggreBuffer.getAs[String](3), row.getAs[String](3)).mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")
    //week_predict_score
    aggreBuffer.update(3, week_predicted_score)


    val days = row.getSeq[String](5)
    val daySet = Set[String]()
    aggreBuffer.getAs[Seq[String]](5).foreach(daySet += _)
    days.foreach(f =>
      daySet += f
    )
    //do_exercise_day
    aggreBuffer.update(5, daySet.toSeq)

    val weekQuestions = row.get(6).asInstanceOf[Seq[Int]].seq
    val weekQuesArr = new ArrayBuffer[Int]()
    aggreBuffer.get(6).asInstanceOf[Seq[Int]].seq.foreach(weekQuesArr += _)
    weekQuestions.foreach(f =>
      weekQuesArr += f
    )

    aggreBuffer.update(6, weekQuesArr.toSeq)
    aggreBuffer.update(7, row.get(7).asInstanceOf[Int].intValue() + aggreBuffer.get(7).asInstanceOf[Int].intValue())

    aggreBuffer.update(8, row.get(8).asInstanceOf[Long].longValue() + aggreBuffer.get(8).asInstanceOf[Long].longValue())

    aggreBuffer.update(9, row.get(9).asInstanceOf[Long].longValue() + aggreBuffer.get(9).asInstanceOf[Long].longValue())

    val error = row.get(10).asInstanceOf[Long].longValue()
    aggreBuffer.update(10, error + aggreBuffer.get(10).asInstanceOf[Long].longValue())

    val weekError = row.get(11).asInstanceOf[Long].longValue()
    aggreBuffer.update(11, weekError + aggreBuffer.get(11).asInstanceOf[Long].longValue())
  }

  override def evaluate(buffer: Row): Any = {

    val total_station_predict_score = buffer.getAs[String](0)
    val do_exercise_num = buffer.getAs[collection.mutable.Set[Int]](1).size
    val cumulative_time = buffer.get(2).asInstanceOf[Int].intValue()
    val week_predict_score = buffer.getAs[String](3)
    val do_exercise_day = buffer.getAs[collection.mutable.Set[String]](5).size

    val week_do_exercise_num = buffer.getAs[collection.mutable.Set[Int]](6).size
    val week_cumulative_time = buffer.get(7).asInstanceOf[Int].intValue()

    val total_correct_num = buffer.get(8).asInstanceOf[Long].longValue()
    val week_correct_num = buffer.get(9).asInstanceOf[Long].longValue()

    val total_undo_num = buffer.get(10).asInstanceOf[Long].longValue()
    val week_undo_num = buffer.get(11).asInstanceOf[Long].longValue()

    Array(
      total_station_predict_score.replaceAll("-1:0:0:0:0_", ""),
      do_exercise_num.toString,
      cumulative_time.toString,
      week_predict_score.toString,
      do_exercise_day.toString,
      week_do_exercise_num.toString,
      week_cumulative_time.toString,
      total_correct_num.toString,
      week_correct_num.toString,
      total_undo_num.toString,
      week_undo_num.toString
    )
  }
}

object PredictedScore {


  /**
    * -1:0:0_-1:0:0
    */
  def getTSPredictScore2Map(str: String): scala.collection.mutable.Map[Int, (Int, Int, Int, Int)] = {

    var mutmap = Map[Int, (Int, Int, Int, Int)]()
    val total_station_predict_score = str

    total_station_predict_score.split("_").foreach(f => {
      val arr = f.split(":").map(Integer.parseInt(_))
      mutmap += (arr(0).intValue() -> (arr(1).intValue(), arr(2).intValue(), arr(3).intValue(), arr(4).intValue()))
    })

    mutmap
  }

  def mergeMap(in: String, buffer: String): Map[Int, (Int, Int, Int, Int)] = {

    val inMap = PredictedScore.getTSPredictScore2Map(in)

    val bufferMap = PredictedScore.getTSPredictScore2Map(buffer)

    inMap.foreach { f =>
      val point = f._1
      val correct = f._2._1
      val total = f._2._2
      val tim = f._2._3
      val undo = f._2._4

      val count = bufferMap.getOrElse(f._1, (0, 0, 0, 0))
      bufferMap += (point -> (count._1 + correct, count._2 + total, count._3 + tim, count._4 + undo))
    }
    bufferMap
  }

  def weekPredictedScore(mutmap: Map[Int, (Int, Int, Int, Int)], map: scala.collection.mutable.Map[Int, (Int, Int, Int, Int)]) = {
    mutmap.foreach { f =>
      val point = f._1

      val correct = f._2._1
      val total = f._2._2
      val tim = f._2._3
      val undo = f._2._4

      val buffer_correct = map.getOrElse(f._1, (0, 0, 0, 0))._1
      val buffer_total = map.getOrElse(f._1, (0, 0, 0, 0))._2
      val buffer_time = map.getOrElse(f._1, (0, 0, 0, 0))._3
      val buffer_undo = map.getOrElse(f._1, (0, 0, 0, 0))._4


      map += (point -> (buffer_correct + correct, buffer_total + total, buffer_time + tim, buffer_undo + undo))
    }

    map.mkString("_")
      .replaceAll(" ", "")
      .replaceAll("->\\(", ":")
      .replaceAll("\\)", "")
      .replaceAll(",", ":")
  }

  /**
    *
    * @param grade
    * @param _type
    * @return
    */
  def getScore(grade: String, _type: Int): Double = {
    var defaultScore = 0.0
    var map = getTSPredictScore2Map(grade)
    _type match {
      case 1 => {
        var score: Double = 0.0
        val changshi = map.getOrElse(392, (0, 0, 0, 0))._1 * 1.0 / map.getOrElse(392, (0, 1, 0, 0))._2 * 1.0
        val yanyu = map.getOrElse(435, (0, 0, 0, 0))._1 * 1.0 / map.getOrElse(435, (0, 1, 0, 0))._2 * 1.0
        val shuliang = map.getOrElse(482, (0, 0, 0, 0))._1 * 1.0 / map.getOrElse(482, (0, 1, 0, 0))._2 * 1.0
        val panduan = map.getOrElse(642, (0, 0, 0, 0))._1 * 1.0 / map.getOrElse(642, (0, 1, 0, 0))._2 * 1.0
        val ziliao = map.getOrElse(754, (0, 0, 0, 0))._1 * 1.0 / map.getOrElse(754, (0, 1, 0, 0))._2 * 1.0


        val Num = map.getOrElse(392, (0, 1, 0, 0))._2 +
          map.getOrElse(435, (0, 1, 0, 0))._2 +
          map.getOrElse(482, (0, 1, 0, 0))._2 +
          map.getOrElse(642, (0, 1, 0, 0))._2 +
          map.getOrElse(754, (0, 1, 0, 0))._2

        if (Num < 150) {
          defaultScore = 0
        } else {
          score += (20.0 / 135.0) * changshi * 100 +
            (40.0 / 135.0) * yanyu * 100 +
            (15.0 / 135.0) * shuliang * 100 +
            (40.0 / 135.0) * panduan * 100 +
            (20.0 / 135.0) * ziliao * 100
          defaultScore = score
        }
      }
      case 2 => {
        var score: Double = 0.0

        var correctNum = map.getOrElse(3125, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(3195, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(3250, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(3280, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(3298, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(3332, (0, 0, 0, 0))._1 * 1.0

        var Num = map.getOrElse(3125, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(3195, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(3250, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(3280, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(3298, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(3332, (0, 1, 0, 0))._2 * 1.0

        if (Num < 150) {
          defaultScore = 0
        } else {
          score += correctNum / Num
          defaultScore = score * 100
        }

      }
      case 3 => {
        var score: Double = 0.0

        var correctNum = map.getOrElse(36667, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36710, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36735, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36748, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36789, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36846, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(36831, (0, 0, 0, 0))._1 * 1.0

        var Num = map.getOrElse(36667, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36710, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36735, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36748, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36789, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36846, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(36831, (0, 1, 0, 0))._2 * 1.0

        if (Num < 150) {
          defaultScore = 0
        } else {
          score += correctNum / Num
          defaultScore = score * 100
        }
      }
      case 100100175 => {
        var score: Double = 0.0

        var correctNum = map.getOrElse(36667, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(37098, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(37099, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(37100, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(37101, (0, 0, 0, 0))._1 * 1.0 +
          map.getOrElse(37175, (0, 0, 0, 0))._1 * 1.0

        var Num = map.getOrElse(36667, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(37098, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(37099, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(37100, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(37101, (0, 1, 0, 0))._2 * 1.0 +
          map.getOrElse(37175, (0, 1, 0, 0))._2 * 1.0

        if (Num < 150) {
          defaultScore = 0
        } else {
          score += correctNum / Num
          defaultScore = score * 100
        }
      }
      case _ => {
        defaultScore
      }
    }

    defaultScore
  }

  def main(args: Array[String]): Unit = {

    val format = "2817632665"


    print( getScore("-1:0:0:0:0_392:1:6:32:0",1))
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
