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
      if (flag){
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
    sparkSession.sql("select userId,points from ztk_answer_card ")
    // val total_station = mongo.select("userId", "subject", "catgory", "expendTime", "createTime", "corrects", "paper.questions", "paper.modules", "StringCount")


    //    ztk_answer_card.printSchema()
    //    ztk_question.printSchema()
    //    val lastWekk = ztk_answer_card.filter(ztk_answer_card("createTime") >= last_week_start.value && ztk_answer_card("createTime") < last_week_end.value)
    //    sparkSession.sql("select userId,count(*) from ztk_answer_card group by userId").repartition(10).rdd.saveAsTextFile(args(0))

    /**
      * 上周数据
      */
    //    lastWekk.rdd.saveAsTextFile(args(0))

  }

}
