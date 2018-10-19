package com.li.ability.assessment


import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map

object AbilityAssessment {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_answer_card"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      .setMaster("local[13]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.uri", inputUrl)
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")
      .set("spark.mongodb.keep_alive_ms", "3600000000000")

    import com.mongodb.spark.sql._
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._
    // spark context
    val sc = sparkSession.sparkContext
    val last_week_start = sc.broadcast(TimeUtils.getLastWeekStartTimeStamp())
    val last_week_end = sc.broadcast(TimeUtils.getLastWeekendTimeStamp())
    // ztk_question
    /**
      * 获得题到知识点的映射
      */
    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "readPreference.name" -> "secondaryPreferred"
        ))).toDF() // Uses the ReadConfig
    ztk_question.createOrReplaceTempView("ztk_question")

    val map = sparkSession.sql("select _id,points from ztk_question").rdd.filter { r =>
      var flag = true

      flag = !r.isNullAt(0) && !r.isNullAt(1) && r.getSeq(1).nonEmpty
      if (flag) {
        flag = r.get(0).getClass.getName match {
          case "java.lang.Double" => false
          case _ => true
        }
      }
      flag
    }.map {
      r =>
        val _id: Int = r.getInt(0)
        val pid: Int = r.getSeq(1).head
        (_id, pid)
    }.collectAsMap()
    //    val q2p = sc.broadcast(map.collectAsMap())
    /**
      * mongo 214024
      * spark 205846
      * the mapping of the knowledge to points
      */
    val q2p = sc.broadcast(map)

    // ztk_answer_card
    val ztk_answer_card = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_answer_card"),
          "readPreference.name" -> "secondaryPreferred"
        )
      )).toDF() // Uses the ReadConfig
    ztk_answer_card.createOrReplaceTempView("ztk_answer_card")
    val card = sparkSession.sql("select userId,corrects,paper.questions,times from ztk_answer_card ").limit(30)

    /**
      * +-------+--------------------+--------------------+--------------------+
      * | userId|            corrects|           questions|               times|
      * +-------+--------------------+--------------------+--------------------+
      * | 420991|[2, 2, 2, 2, 2, 2...|[55309, 55308, 55...|[1, 1, 1, 1, 1, 0...|
      * | 420991|[2, 2, 2, 2, 2, 1...|[48997, 48998, 33...|[8, 5, 1, 1, 1, 1...|
      * |7741045|[2, 2, 2, 1, 2, 2...|[55014, 55010, 55...|[0, 0, 0, 0, 0, 0...|
      * |7741045|[2, 1, 1, 1, 2, 2...|[42063, 40509, 41...|[4, 2, 3, 3, 9, 3...|
      * | 697154|[1, 2, 2, 0, 2, 1...|[31770, 33252, 33...|[1, 5, 1, 0, 6, 1...|
      * |7792877|[1, 1, 1, 1, 1, 1...|[38859, 38860, 38...|[74, 37, 24, 18, ...|
      * |7741045|[0, 0, 0, 0, 0, 0...|[49585, 51696, 48...|[0, 0, 0, 0, 0, 0...|
      * |7792897|[0, 0, 0, 0, 0, 0...|[50802, 52672, 53...|[0, 0, 0, 0, 0, 0...|
      * | 947136|[1, 1, 1, 2, 1, 1...|[34670, 31402, 33...|[1, 2, 1, 1, 2, 0...|
      * |7792929|[2, 2, 2, 2, 1, 2...|[39380, 39381, 39...|[5, 2, 1, 1, 1, 0...|
      * |7792929|[2, 2, 2, 1, 2, 2...|[37632, 37633, 37...|[10, 5, 3, 2, 2, ...|
      * |7792929|[0, 0, 0, 0, 0, 0...|[37632, 37633, 37...|[0, 0, 0, 0, 0, 0...|
      * |7792929|[0, 0, 0, 0, 0, 0...|[37632, 37633, 37...|[0, 0, 0, 0, 0, 0...|
      * |7792929|[2, 1, 1, 2, 2, 0...|[39860, 39861, 39...|[5, 2, 1, 1, 1, 0...|
      * |7792929|[2, 2, 1, 0, 0, 0...|[40085, 40086, 40...|[9, 4, 3, 0, 0, 0...|
      * |3009998|[0, 0, 0, 0, 1, 0...|[51987, 47987, 49...|[0, 0, 0, 0, 9, 0...|
      * |3009998|[0, 0, 0, 0, 0, 0...|[50802, 50798, 50...|[0, 0, 0, 0, 0, 0...|
      * |7774833|[1, 2, 2, 1, 1, 1...|[48976, 53658, 48...|[1, 1, 1, 1, 8, 1...|
      * |7792929|[2, 1, 1, 2, 2, 1...|[37632, 37633, 37...|[4, 2, 1, 1, 0, 0...|
      * | 420991|[2, 2, 2, 2, 2, 2...|[33310, 33304, 33...|[1, 2, 3, 2, 1, 1...|
      * |7792929|[1, 2, 2, 2, 2, 1...|[42517, 42518, 42...|[5, 2, 1, 1, 1, 0...|
      * |3009998|[1, 2, 1, 0, 0, 0...|[42517, 42518, 42...|[29, 14, 9, 0, 0,...|
      * |3009998|[0, 0, 0, 0, 0, 0...|[54762, 49534, 49...|[0, 0, 0, 0, 0, 0...|
      * |7774833|[2, 0, 0, 0, 0, 0...|[38789, 38790, 38...|[180, 0, 0, 0, 0,...|
      * |7792929|[2, 2, 2, 2, 2, 2...|[40307, 40308, 40...|[8, 4, 2, 2, 1, 1...|
      * | 420991|[2, 1, 2, 2, 2, 1...|[48958, 33644, 33...|[1, 18, 1, 3, 7, ...|
      * |7792929|[2, 2, 1, 2, 1, 2...|[38578, 38579, 38...|[7, 3, 2, 1, 1, 1...|
      * |7792929|[2, 2, 2, 2, 2, 2...|[38067, 38068, 38...|[30, 15, 10, 7, 6...|
      * |7792929|[2, 2, 2, 2, 2, 2...|[37942, 37943, 37...|[19, 9, 6, 4, 3, ...|
      * | 420991|[1, 1, 1, 1, 2, 2...|[55296, 55292, 55...|[16, 0, 0, 8, 0, ...|
      * +-------+--------------------+--------------------+--------------------+
      */
    // val total_station = mongo.select("userId", "subject", "catgory", "expendTime", "createTime", "corrects", "paper.questions", "paper.modules", "StringCount")








    /**
      * 上周数据
      */
    card.show(30)

  }

}
